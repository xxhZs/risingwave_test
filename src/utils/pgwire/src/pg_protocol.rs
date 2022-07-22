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
use std::io::{self, Error as IoError, ErrorKind};
use std::str::Utf8Error;
use std::sync::Arc;
use std::{str, vec};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::error::{PsqlError, PsqlResult};
use crate::pg_extended::{PgPortal, PgStatement};
use crate::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use crate::pg_message::{
    BeCommandCompleteMessage, BeMessage, BeParameterStatusMessage, FeBindMessage, FeCloseMessage,
    FeDescribeMessage, FeExecuteMessage, FeMessage, FeParseMessage, FePasswordMessage,
    FeStartupMessage,
};
use crate::pg_response::PgResponse;
use crate::pg_server::{Session, SessionManager, UserAuthenticator};
use crate::types::Row;

/// The state machine for each psql connection.
/// Read pg messages from tcp stream and write results back.
pub struct PgProtocol<S, SM>
where
    SM: SessionManager,
{
    /// Used for write/read message in tcp connection.
    stream: S,
    /// Write into buffer before flush to stream.
    buf_out: BytesMut,
    /// Current states of pg connection.
    state: PgProtocolState,
    /// Whether the connection is terminated.
    is_terminate: bool,

    session_mgr: Arc<SM>,
    session: Option<Arc<SM::Session>>,
}

/// States flow happened from top to down.
enum PgProtocolState {
    Startup,
    Regular,
}

// Truncate 0 from C string in Bytes and stringify it (returns slice, no allocations)
// PG protocol strings are always C strings.
pub fn cstr_to_str(b: &Bytes) -> Result<&str, Utf8Error> {
    let without_null = if b.last() == Some(&0) {
        &b[..b.len() - 1]
    } else {
        &b[..]
    };
    std::str::from_utf8(without_null)
}

impl<S, SM> PgProtocol<S, SM>
where
    S: AsyncWrite + AsyncRead + Unpin,
    SM: SessionManager,
{
    pub fn new(stream: S, session_mgr: Arc<SM>) -> Self {
        Self {
            stream,
            is_terminate: false,
            state: PgProtocolState::Startup,
            buf_out: BytesMut::with_capacity(10 * 1024),
            session_mgr,
            session: None,
        }
    }

    pub async fn process(
        &mut self,
        unnamed_statement: &mut PgStatement,
        unnamed_portal: &mut PgPortal,
        named_statements: &mut HashMap<String, PgStatement>,
        named_portals: &mut HashMap<String, PgPortal>,
    ) -> bool {
        if self
            .do_process(
                unnamed_statement,
                unnamed_portal,
                named_statements,
                named_portals,
            )
            .await
        {
            return true;
        }

        self.is_terminate()
    }

    async fn do_process(
        &mut self,
        unnamed_statement: &mut PgStatement,
        unnamed_portal: &mut PgPortal,
        named_statements: &mut HashMap<String, PgStatement>,
        named_portals: &mut HashMap<String, PgPortal>,
    ) -> bool {
        match self
            .do_process_inner(
                unnamed_statement,
                unnamed_portal,
                named_statements,
                named_portals,
            )
            .await
        {
            Ok(v) => {
                return v;
            }
            Err(e) => {
                tracing::error!("Error: {}", e);
                match e {
                    PsqlError::SslError(io_err) | PsqlError::IoError(io_err) => {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            return true;
                        }
                    }

                    PsqlError::StartupError(_) | PsqlError::PasswordError(_) => {
                        self.write_message_for_error(&BeMessage::ErrorResponse(Box::new(e)));
                        return true;
                    }

                    PsqlError::ReadMsgError(io_err) => {
                        if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                            return true;
                        }
                        self.write_message_for_error(&BeMessage::ErrorResponse(Box::new(io_err)));
                        self.write_message_for_error(&BeMessage::ReadyForQuery);
                    }

                    PsqlError::QueryError(_) => {
                        self.write_message_for_error(&BeMessage::ErrorResponse(Box::new(e)));
                        self.write_message_for_error(&BeMessage::ReadyForQuery);
                    }

                    PsqlError::CloseError(_)
                    | PsqlError::DescribeError(_)
                    | PsqlError::ParseError(_)
                    | PsqlError::BindError(_)
                    | PsqlError::ExecuteError(_) => {
                        self.write_message_for_error(&BeMessage::ErrorResponse(Box::new(e)));
                    }

                    // Never reach this.
                    PsqlError::CancelMsg(_) => todo!(),
                }
                self.flush_for_error().await;
            }
        };
        false
    }

    async fn do_process_inner(
        &mut self,
        unnamed_statement: &mut PgStatement,
        unnamed_portal: &mut PgPortal,
        named_statements: &mut HashMap<String, PgStatement>,
        named_portals: &mut HashMap<String, PgPortal>,
    ) -> PsqlResult<bool> {
        let msg = self.read_message().await?;
        match msg {
            FeMessage::Ssl => {
                self.process_ssl_msg()?;
            }
            FeMessage::Startup(msg) => {
                self.process_startup_msg(msg)?;
                self.state = PgProtocolState::Regular;
            }
            FeMessage::Password(msg) => {
                self.process_password_msg(msg)?;
                self.state = PgProtocolState::Regular;
            }
            FeMessage::Query(query_msg) => {
                self.process_query_msg(query_msg.get_sql()).await?;
            }
            FeMessage::CancelQuery => {
                self.process_cancel_msg()?;
            }
            FeMessage::Terminate => {
                self.process_terminate();
            }
            FeMessage::Parse(m) => {
                self.process_parse_msg(m, named_statements, unnamed_statement)
                    .await?;
            }
            FeMessage::Bind(m) => {
                self.process_bind_msg(
                    m,
                    named_statements,
                    unnamed_statement,
                    named_portals,
                    unnamed_portal,
                )
                .await?;
            }
            FeMessage::Execute(m) => {
                self.process_execute_msg(m, named_portals, unnamed_portal)
                    .await?;
            }
            FeMessage::Describe(m) => {
                self.process_describe_msg(
                    m,
                    named_statements,
                    unnamed_statement,
                    named_portals,
                    unnamed_portal,
                )
                .await?;
            }
            FeMessage::Sync => {
                self.write_message(&BeMessage::ReadyForQuery).await?;
            }
            FeMessage::Close(m) => {
                self.process_close_msg(m, named_statements, named_portals)
                    .await?;
            }
        }
        self.flush().await?;
        Ok(false)
    }

    async fn read_message(&mut self) -> PsqlResult<FeMessage> {
        match self.state {
            PgProtocolState::Startup => FeStartupMessage::read(&mut self.stream)
                .await
                .map_err(PsqlError::ReadMsgError),
            PgProtocolState::Regular => FeMessage::read(&mut self.stream)
                .await
                .map_err(PsqlError::ReadMsgError),
        }
    }

    fn process_ssl_msg(&mut self) -> PsqlResult<()> {
        self.write_message_no_flush(&BeMessage::EncryptionResponse)
            .map_err(PsqlError::SslError)?;
        Ok(())
    }

    fn process_startup_msg(&mut self, msg: FeStartupMessage) -> PsqlResult<()> {
        let db_name = msg
            .config
            .get("database")
            .cloned()
            .unwrap_or_else(|| "dev".to_string());
        let user_name = msg
            .config
            .get("user")
            .cloned()
            .unwrap_or_else(|| "root".to_string());

        let session = self
            .session_mgr
            .connect(&db_name, &user_name)
            .map_err(PsqlError::StartupError)?;
        match session.user_authenticator() {
            UserAuthenticator::None => {
                self.write_message_no_flush(&BeMessage::AuthenticationOk)
                    .map_err(|err| PsqlError::StartupError(Box::new(err)))?;
                self.write_parameter_status_msg_no_flush()
                    .map_err(|err| PsqlError::StartupError(Box::new(err)))?;
                self.write_message_no_flush(&BeMessage::ReadyForQuery)
                    .map_err(|err| PsqlError::StartupError(Box::new(err)))?;
            }
            UserAuthenticator::ClearText(_) => {
                self.write_message_no_flush(&BeMessage::AuthenticationCleartextPassword)
                    .map_err(|err| PsqlError::StartupError(Box::new(err)))?;
            }
            UserAuthenticator::MD5WithSalt { salt, .. } => {
                self.write_message_no_flush(&BeMessage::AuthenticationMD5Password(salt))
                    .map_err(|err| PsqlError::StartupError(Box::new(err)))?;
            }
        }
        self.session = Some(session);
        Ok(())
    }

    fn process_password_msg(&mut self, msg: FePasswordMessage) -> PsqlResult<()> {
        let authenticator = self.session.as_ref().unwrap().user_authenticator();
        if !authenticator.authenticate(&msg.password) {
            return Err(PsqlError::PasswordError(IoError::new(
                ErrorKind::InvalidInput,
                "Invalid password",
            )));
        }
        self.write_message_no_flush(&BeMessage::AuthenticationOk)
            .map_err(PsqlError::PasswordError)?;
        self.write_parameter_status_msg_no_flush()
            .map_err(PsqlError::PasswordError)?;
        self.write_message_no_flush(&BeMessage::ReadyForQuery)
            .map_err(PsqlError::PasswordError)?;
        Ok(())
    }

    fn process_cancel_msg(&mut self) -> PsqlResult<()> {
        self.write_message_no_flush(&BeMessage::ErrorResponse(Box::new(PsqlError::cancel())))?;
        Ok(())
    }

    async fn process_query_msg(&mut self, query_string: io::Result<&str>) -> PsqlResult<()> {
        let sql = query_string.map_err(|err| PsqlError::QueryError(Box::new(err)))?;
        tracing::trace!("(simple query)receive query: {}", sql);

        let session = self.session.clone().unwrap();
        // execute query
        let res = session
            .run_statement(sql)
            .await
            .map_err(|err| PsqlError::QueryError(err))?;

        if res.is_empty() {
            self.write_message_no_flush(&BeMessage::EmptyQueryResponse)?;
        } else if res.is_query() {
            self.process_response_results(res, None).await?;
        } else {
            self.write_message_no_flush(&BeMessage::CommandComplete(BeCommandCompleteMessage {
                stmt_type: res.get_stmt_type(),
                notice: res.get_notice(),
                rows_cnt: res.get_effected_rows_cnt(),
            }))?;
        }

        self.write_message_no_flush(&BeMessage::ReadyForQuery)?;
        Ok(())
    }

    fn process_terminate(&mut self) {
        self.is_terminate = true;
    }

    async fn process_parse_msg(
        &mut self,
        msg: FeParseMessage,
        named_statements: &mut HashMap<String, PgStatement>,
        unnamed_statement: &mut PgStatement,
    ) -> PsqlResult<()> {
        let query = cstr_to_str(&msg.query_string).unwrap();
        tracing::trace!("(extended query)parse query: {}", query);
        // 1. Create the types description.
        let type_ids = msg.type_ids;
        let types: Vec<TypeOid> = type_ids
            .into_iter()
            .map(|x| TypeOid::as_type(x).unwrap())
            .collect();

        // 2. Create the row description.

        let rows: Vec<PgFieldDescriptor> = if query.starts_with("SELECT")
            || query.starts_with("select")
        {
            if types.is_empty() {
                let session = self.session.clone().unwrap();
                session
                    .infer_return_type(query)
                    .await
                    .map_err(PsqlError::ParseError)?
            } else {
                query
                    .split(&[' ', ',', ';'])
                    .skip(1)
                    .into_iter()
                    .take_while(|x| !x.is_empty())
                    .map(|x| {
                        // NOTE: Assume all output are generic params.
                        let str = x.strip_prefix('$').unwrap();
                        // NOTE: Assume all generic are valid.
                        let v: i32 = str.parse().unwrap();
                        assert!(v.is_positive());
                        v
                    })
                    .map(|x| {
                        // NOTE Make sure the type_description include all generic parameter
                        // description we needed.
                        assert!(((x - 1) as usize) < types.len());
                        PgFieldDescriptor::new(String::new(), types[(x - 1) as usize].to_owned())
                    })
                    .collect()
            }
        } else {
            vec![]
        };

        // 3. Create the statement.
        let statement = PgStatement::new(
            cstr_to_str(&msg.statement_name).unwrap().to_string(),
            msg.query_string,
            types,
            rows,
        );

        // 4. Insert the statement.
        let name = statement.name();
        if name.is_empty() {
            *unnamed_statement = statement;
        } else {
            named_statements.insert(name, statement);
        }
        self.write_message(&BeMessage::ParseComplete).await?;
        Ok(())
    }

    async fn process_bind_msg(
        &mut self,
        msg: FeBindMessage,
        named_statements: &mut HashMap<String, PgStatement>,
        unnamed_statement: &mut PgStatement,
        named_portals: &mut HashMap<String, PgPortal>,
        unnamed_portal: &mut PgPortal,
    ) -> PsqlResult<()> {
        let statement_name = cstr_to_str(&msg.statement_name).unwrap().to_string();
        // 1. Get statement.
        let statement = if statement_name.is_empty() {
            unnamed_statement
        } else {
            // NOTE Error handle method may need to modified.
            // Postgresql doc needs write ErrorResponse if name not found. We may revisit
            // this part if needed.
            named_statements.get(&statement_name).expect("statement_name managed by client_driver, hence assume statement name always valid.")
        };

        // 2. Instance the statement to get the portal.
        let portal_name = cstr_to_str(&msg.portal_name).unwrap().to_string();
        let portal = statement
            .instance::<SM>(
                self.session.clone().unwrap(),
                portal_name.clone(),
                &msg.params,
                msg.result_format_code,
            )
            .await
            .unwrap();

        // 3. Insert the Portal.
        if portal_name.is_empty() {
            *unnamed_portal = portal;
        } else {
            named_portals.insert(portal_name, portal);
        }
        self.write_message(&BeMessage::BindComplete).await?;
        Ok(())
    }

    async fn process_execute_msg(
        &mut self,
        msg: FeExecuteMessage,
        named_portals: &mut HashMap<String, PgPortal>,
        unnamed_portal: &mut PgPortal,
    ) -> PsqlResult<()> {
        // 1. Get portal.
        let portal_name = cstr_to_str(&msg.portal_name).unwrap().to_string();
        let portal = if msg.portal_name.is_empty() {
            unnamed_portal
        } else {
            // NOTE Error handle need modify later.
            named_portals.get_mut(&portal_name).expect(
                "portal_name managed by client_driver, hence assume portal name always valid",
            )
        };

        tracing::trace!(
            "(extended query)execute query: {}",
            cstr_to_str(&portal.query_string()).unwrap()
        );

        // 2. Execute instance statement using portal.
        let session = self.session.clone().unwrap();
        let res = portal
            .execute::<SM>(session, msg.max_rows.try_into().unwrap())
            .await
            .map_err(PsqlError::ExecuteError)?;

        if res.is_empty() {
            self.write_message_no_flush(&BeMessage::EmptyQueryResponse)?;
        } else if res.is_query() {
            self.process_response_results(res, Some(portal.result_format()))
                .await?;
        } else {
            self.write_message_no_flush(&BeMessage::CommandComplete(BeCommandCompleteMessage {
                stmt_type: res.get_stmt_type(),
                notice: res.get_notice(),
                rows_cnt: res.get_effected_rows_cnt(),
            }))?;
        }

        // NOTE there is no ReadyForQuery message.
        Ok(())
    }

    async fn process_describe_msg(
        &mut self,
        msg: FeDescribeMessage,
        named_statements: &mut HashMap<String, PgStatement>,
        unnamed_statement: &mut PgStatement,
        named_portals: &mut HashMap<String, PgPortal>,
        unnamed_portal: &mut PgPortal,
    ) -> PsqlResult<()> {
        // m.kind indicates the Describe type:
        //  b'S' => Statement
        //  b'P' => Portal
        assert!(msg.kind == b'S' || msg.kind == b'P');
        if msg.kind == b'S' {
            let name = cstr_to_str(&msg.query_name).unwrap().to_string();
            let statement = if name.is_empty() {
                unnamed_statement
            } else {
                // NOTE Error handle need modify later.
                named_statements.get(&name).unwrap()
            };

            // 1. Send parameter description.
            self.write_message(&BeMessage::ParameterDescription(&statement.type_desc()))
                .await?;

            // 2. Send row description.
            self.write_message(&BeMessage::RowDescription(&statement.row_desc()))
                .await?;
        } else if msg.kind == b'P' {
            let name = cstr_to_str(&msg.query_name).unwrap().to_string();
            let portal = if name.is_empty() {
                unnamed_portal
            } else {
                // NOTE Error handle need modify later.
                named_portals.get(&name).unwrap()
            };

            // 3. Send row description.
            self.write_message(&BeMessage::RowDescription(&portal.row_desc()))
                .await?;
        }
        Ok(())
    }

    async fn process_close_msg(
        &mut self,
        msg: FeCloseMessage,
        named_statements: &mut HashMap<String, PgStatement>,
        named_portals: &mut HashMap<String, PgPortal>,
    ) -> PsqlResult<()> {
        let name = cstr_to_str(&msg.query_name).unwrap().to_string();
        assert!(msg.kind == b'S' || msg.kind == b'P');
        if msg.kind == b'S' {
            named_statements.remove_entry(&name);
        } else if msg.kind == b'P' {
            named_portals.remove_entry(&name);
        }
        self.write_message(&BeMessage::CloseComplete).await?;
        Ok(())
    }

    async fn process_response_results(
        &mut self,
        res: PgResponse,

        // extended:None indicates simple query mode.
        // extended:Some(result_format_code) indicates extended query mode.
        extended: Option<bool>,
    ) -> Result<(), IoError> {
        // The possible responses to Execute are the same as those described above for queries
        // issued via simple query protocol, except that Execute doesn't cause ReadyForQuery or
        // RowDescription to be issued.
        // Quoted from: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY
        if extended.is_none() {
            self.write_message(&BeMessage::RowDescription(&res.get_row_desc()))
                .await?;
        }

        let mut rows_cnt = 0;

        // Simple query mode(default format: 'TEXT') or result_format is 'TEXT'.
        if extended.is_none() || !extended.unwrap() {
            let iter = res.iter();
            for val in iter {
                self.write_message(&BeMessage::DataRow(val)).await?;
                rows_cnt += 1;
            }
        } else {
            let iter = res.iter();
            let row_description = res.get_row_desc();
            for val in iter {
                let val = Self::covert_to_binary_format(val, &row_description);
                self.write_message(&BeMessage::BinaryRow(&val)).await?;
                rows_cnt += 1;
            }
        }

        // If has rows limit, it must be extended mode.
        // If Execute terminates before completing the execution of a portal (due to reaching a
        // nonzero result-row count), it will send a PortalSuspended message; the appearance of this
        // message tells the frontend that another Execute should be issued against the same portal
        // to complete the operation. The CommandComplete message indicating completion of the
        // source SQL command is not sent until the portal's execution is completed.
        // Quote from: https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-FLOW-EXT-QUERY:~:text=Once%20a%20portal,ErrorResponse%2C%20or%20PortalSuspended
        if extended.is_none() || res.is_row_end() {
            self.write_message_no_flush(&BeMessage::CommandComplete(BeCommandCompleteMessage {
                stmt_type: res.get_stmt_type(),
                notice: res.get_notice(),
                rows_cnt,
            }))?;
        } else {
            self.write_message(&BeMessage::PortalSuspended).await?;
        }
        Ok(())
    }

    fn is_terminate(&self) -> bool {
        self.is_terminate
    }

    fn write_parameter_status_msg_no_flush(&mut self) -> io::Result<()> {
        self.write_message_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::ClientEncoding("UTF8"),
        ))?;
        self.write_message_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::StandardConformingString("on"),
        ))?;
        self.write_message_no_flush(&BeMessage::ParameterStatus(
            BeParameterStatusMessage::ServerVersion("9.5.0"),
        ))?;
        Ok(())
    }

    // The following functions are used to response something error to client.
    // The write() interface of this kind of message must be send sucesssfully or "unwrap" when it
    // failed. Hence we can dirtyly unwrap write_message_no_flush, it must return Ok(),
    // otherwise system will panic and it never return.
    fn write_message_for_error(&mut self, message: &BeMessage<'_>) {
        self.write_message_no_flush(message).unwrap_or_else(|e| {
            tracing::error!("Error: {}", e);
        });
    }

    // The following functions are used to response something error to client.
    // If flush fail, it logs internally and don't report to user.
    // This approach is equal to the past.
    async fn flush_for_error(&mut self) {
        self.flush().await.unwrap_or_else(|e| {
            tracing::error!("flush error: {}", e);
        });
    }

    fn write_message_no_flush(&mut self, message: &BeMessage<'_>) -> io::Result<()> {
        BeMessage::write(&mut self.buf_out, message)
    }

    async fn write_message(&mut self, message: &BeMessage<'_>) -> io::Result<()> {
        self.write_message_no_flush(message)?;
        self.flush().await?;
        Ok(())
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.stream.write_all(&self.buf_out).await?;
        self.buf_out.clear();
        self.stream.flush().await?;
        Ok(())
    }

    /// covert_the row to binary format.
    fn covert_to_binary_format(row: &Row, row_desc: &Vec<PgFieldDescriptor>) -> Vec<Option<Bytes>> {
        assert_eq!(row.len(), row_desc.len());

        let len = row.len();
        let mut res = Vec::with_capacity(len);

        for idx in 0..len {
            let value = &row[idx];
            let type_oid = row_desc[idx].get_type_oid();

            match value {
                None => res.push(None),
                Some(value) => match type_oid {
                    TypeOid::Boolean => todo!(),
                    TypeOid::BigInt => todo!(),
                    TypeOid::SmallInt => todo!(),
                    TypeOid::Int => {
                        let value: i32 = value.parse().unwrap();
                        res.push(Some(value.to_be_bytes().to_vec().into()));
                    }
                    TypeOid::Float4 => todo!(),
                    TypeOid::Float8 => todo!(),
                    TypeOid::Varchar => res.push(Some(value.clone().into())),
                    TypeOid::Date => todo!(),
                    TypeOid::Time => todo!(),
                    TypeOid::Timestamp => todo!(),
                    TypeOid::Timestampz => todo!(),
                    TypeOid::Decimal => todo!(),
                    TypeOid::Interval => todo!(),
                },
            }
        }
        res
    }
}
