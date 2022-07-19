// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{bail, Context};
use log::{debug, error, info};
use tokio::process::Command;

use crate::schedule::TestResult::{Different, Same};
use crate::{init_env, DatabaseMode, FileManager, Opts, Psql};

/// Result of each test case.
#[derive(PartialEq)]
enum TestResult {
    /// Execution of the test case succeeded, and results are same.
    Same,
    /// Execution of the test case succeeded, but outputs are different from expected result.
    Different,
}

struct TestCase {
    test_name: String,
    opts: Opts,
    psql: Arc<Psql>,
    file_manager: Arc<FileManager>,
}

pub(crate) struct Schedule {
    opts: Opts,
    file_manager: Arc<FileManager>,
    psql: Arc<Psql>,
    /// Schedules of test names.
    ///
    /// Each item is called a parallel schedule, which runs parallel.
    schedules: Vec<Vec<String>>,
}

impl Schedule {
    pub(crate) fn new(opts: Opts) -> anyhow::Result<Self> {
        Ok(Self {
            opts: opts.clone(),
            file_manager: Arc::new(FileManager::new(opts.clone())),
            psql: Arc::new(Psql::new(opts.clone())),
            schedules: Schedule::parse_from(opts.schedule_file_path())?,
        })
    }

    async fn do_init(self) -> anyhow::Result<Self> {
        init_env();

        self.file_manager.init()?;
        self.psql.init().await?;

        Ok(self)
    }

    fn parse_from<P: AsRef<Path>>(path: P) -> anyhow::Result<Vec<Vec<String>>> {
        let file = File::options()
            .read(true)
            .open(path.as_ref())
            .with_context(|| format!("Failed to open schedule file: {:?}", path.as_ref()))?;

        let reader = BufReader::new(file);
        let mut schedules = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.starts_with("test: ") {
                schedules.push(
                    line[5..]
                        .split_whitespace()
                        .map(ToString::to_string)
                        .collect(),
                );
                debug!("Add one parallel schedule: {:?}", schedules.last().unwrap());
            }
        }

        Ok(schedules)
    }

    /// Run all test schedules.
    ///
    /// # Returns
    ///
    /// `Ok` If no error happens and all outputs are expected,
    /// `Err` If any error happens, or some outputs are unexpected. Details are logged in log file.
    pub(crate) async fn run(self) -> anyhow::Result<()> {
        let s = self.do_init().await?;
        s.do_run().await
    }

    async fn do_run(self) -> anyhow::Result<()> {
        let mut different_tests = Vec::new();
        for parallel_schedule in &self.schedules {
            info!("Running parallel schedule: {:?}", parallel_schedule);
            let ret = self
                .run_one_schedule(parallel_schedule.iter().map(String::as_str))
                .await?;

            let mut diff_test = ret
                .iter()
                .filter(|(_test_name, test_result)| **test_result == Different)
                .map(|t| t.0.clone())
                .collect::<Vec<String>>();

            if !diff_test.is_empty() {
                error!(
                    "Parallel schedule failed, these tests are different: {:?}",
                    diff_test
                );
                different_tests.append(&mut diff_test);
            } else {
                info!("Parallel schedule succeeded!");
            }
        }

        if !different_tests.is_empty() {
            info!(
                "Risingwave regress tests failed, these tests are different from expected output: {:?}",
                different_tests
            );
            bail!(
                "Risingwave regress tests failed, these tests are different from expected output: {:?}",
                different_tests
            )
        } else {
            info!("Risingwave regress tests passed.");
            Ok(())
        }
    }

    async fn run_one_schedule(
        &self,
        tests: impl Iterator<Item = &str>,
    ) -> anyhow::Result<HashMap<String, TestResult>> {
        let mut join_handles = HashMap::new();

        for test_name in tests {
            let test_case = self.create_test_case(test_name);
            let join_handle = tokio::spawn(async move { test_case.run().await });
            join_handles.insert(test_name, join_handle);
        }

        let mut result = HashMap::new();

        for (test_name, join_handle) in join_handles {
            let ret = join_handle
                .await
                .with_context(|| format!("Running test case {} panicked!", test_name))??;

            result.insert(test_name.to_string(), ret);
        }

        Ok(result)
    }

    fn create_test_case(&self, test_name: &str) -> TestCase {
        TestCase {
            test_name: test_name.to_string(),
            opts: self.opts.clone(),
            psql: self.psql.clone(),
            file_manager: self.file_manager.clone(),
        }
    }
}

impl TestCase {
    async fn run(self) -> anyhow::Result<TestResult> {
        let mut command = Command::new("psql");
        command.env(
            "PGAPPNAME",
            format!("risingwave_regress/{}", self.test_name),
        );

        let host = &self.opts.host();
        let port = &self.opts.port().to_string();
        let database_name = self.opts.database_name();
        let pg_user_name = self.opts.pg_user_name();
        let args: Vec<&str> = vec![
            "-X",
            "-a",
            "-q",
            "-h",
            host,
            "-p",
            port,
            "-d",
            database_name,
            "-U",
            pg_user_name,
            "-v",
            "HIDE_TABLEAM=on",
            "-v",
            "HIDE_TOAST_COMPRESSION=on",
        ];
        println!("Ready to run command:\npsql {}", args.join(" "));
        command.args(args);

        let mut extra_lines_added_to_input = vec![];

        let input_path = self.file_manager.source_of(&self.test_name)?;
        let mut input_file = File::options()
            .read(true)
            .open(&input_path)
            .with_context(|| format!("Failed to open {:?} for read.", input_path))?;
        let input_file = match self.opts.database_mode() {
            DatabaseMode::Risingwave => {
                extra_lines_added_to_input.push("SET RW_IMPLICIT_FLUSH TO true;");
                let mut tmp_file = tempfile::tempfile()?;
                for extra_line in &extra_lines_added_to_input {
                    tmp_file.write_all(extra_line.as_bytes())?;
                }
                let mut buffer = String::new();
                // read data from the whole input file and write them into the tmp_file
                input_file.read_to_string(&mut buffer)?;
                tmp_file.write_all(buffer.as_bytes())?;
                // We reset the position because tmp_file will be read.
                tmp_file.seek(SeekFrom::Start(0))?;
                tmp_file
            }
            DatabaseMode::PostgreSQL => input_file,
        };
        command.stdin(input_file);

        let expected_output_file = self.file_manager.expected_output_of(&self.test_name)?;

        let output_path = self.file_manager.output_of(&self.test_name)?;
        let output_file = File::options()
            .create_new(true)
            .write(true)
            .open(&output_path)
            .with_context(|| format!("Failed to create {:?} for writing output.", output_path))?;
        command.stdout(
            output_file
                .try_clone()
                .with_context(|| format!("Failed to clone output file: {:?}", output_path))?,
        );
        command.stderr(output_file);

        info!(
            "Starting to execute test case: {}, command: {:?}",
            self.test_name, command
        );
        let status = command
            .spawn()
            .with_context(|| format!("Failed to spawn child for test cast: {}", self.test_name))?
            .wait()
            .await
            .with_context(|| {
                format!("Failed to wait for finishing test cast: {}", self.test_name)
            })?;

        if !status.success() {
            error!(
                "Execution of test case {} failed, reason: {:?}",
                self.test_name, status
            );
            bail!(
                "Execution of test case {} failed, reason: {:?}",
                self.test_name,
                status
            );
        }

        let expected_output = read_lines(&expected_output_file, 0).with_context(|| {
            format!(
                "Failed to read expected output file: {:?}",
                expected_output_file
            )
        })?;

        // Since we may add some lines to the beginning of each file, and these lines will be
        // part of the actual output. So here we ignore those lines.
        let actual_output = read_lines(&output_path, extra_lines_added_to_input.len())
            .with_context(|| format!("Failed to read actual output file: {:?}", output_path))?;

        if expected_output == actual_output {
            Ok(Same)
        } else {
            // If the output is not as expected, we output the diff for easier human reading.
            let diff_path = self.file_manager.diff_of(&self.test_name)?;
            let mut diff_file = File::options()
                .create_new(true)
                .write(true)
                .open(&diff_path)
                .with_context(|| format!("Failed to create {:?} for writing diff.", diff_path))?;
            write!(
                diff_file,
                "{}",
                format_diff(&expected_output, &actual_output)
            )?;
            Ok(Different)
        }
    }
}

/// This function ignores the comments and empty lines. They are not compared between
/// expected output file and actual output file.
fn read_lines<P>(filename: P, mut skip: usize) -> std::io::Result<String>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    let lines = std::io::BufReader::new(file).lines();
    let mut res: String = String::new();
    for line in lines {
        let line = line?;
        if !line.starts_with("--") && !line.is_empty() {
            if skip == 0 {
                res.push_str(&line);
                res.push('\n');
            } else {
                skip -= 1;
            }
        }
    }
    Ok(res)
}

fn format_diff(expected_output: &String, actual_output: &String) -> String {
    use std::fmt::Write;

    use similar::{ChangeTag, TextDiff};
    let diff = TextDiff::from_lines(expected_output, actual_output);

    let mut diff_str = "".to_string();
    for change in diff.iter_all_changes() {
        let sign = match change.tag() {
            ChangeTag::Delete => "-",
            ChangeTag::Insert => "+",
            ChangeTag::Equal => " ",
        };
        write!(diff_str, "{}{}", sign, change).unwrap();
    }
    diff_str
}
