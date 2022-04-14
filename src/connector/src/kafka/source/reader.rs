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
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use rdkafka::config::RDKafkaLogLevel;
use rdkafka::consumer::{DefaultConsumerContext, StreamConsumer};
use rdkafka::ClientConfig;
use futures::StreamExt;
use risingwave_common::error::ErrorCode::{InternalError, ProtocolError};
use risingwave_common::error::RwError;
use crate::AnyhowProperties;

use crate::base::{InnerMessage, SourceReader};
use crate::kafka::split::KafkaSplit;
use crate::kafka::{KAFKA_CONFIG_BROKER_KEY, KAFKA_CONFIG_CONSUME_GROUP, KAFKA_CONFIG_SCAN_STARTUP_MODE};

const KAFKA_MAX_FETCH_MESSAGES: usize = 1024;

pub struct KafkaSplitReader {
    consumer: Arc<StreamConsumer<DefaultConsumerContext>>,
}

#[async_trait]
impl SourceReader for KafkaSplitReader {
    async fn next(&mut self) -> Result<Option<Vec<InnerMessage>>> {
        // todo: use partition queue
        let mut stream = self
            .consumer
            .stream()
            .ready_chunks(KAFKA_MAX_FETCH_MESSAGES);

        stream.next().await.map(|chunk| {
            chunk
                .into_iter()
                .map(|msg| msg.map_err(|e| anyhow!(e)).map(InnerMessage::from))
                .collect::<Result<Vec<InnerMessage>>>()
        }).transpose()
    }

    async fn new(
        properties: HashMap<String, String>,
        _state: Option<crate::ConnectorState>,
    ) -> Result<Self>
        where
            Self: Sized,
    {
        let properties = AnyhowProperties::new(properties);
        let broker = properties.get_kafka(KAFKA_CONFIG_BROKER_KEY)?;
        let mut config = ClientConfig::new();

        let auto_offset_reset = match properties.get_kafka(KAFKA_CONFIG_SCAN_STARTUP_MODE).ok() {
            None => "smallest",
            Some(config) => match config.as_str() {
                "earliest" => "smallest",
                "latest" => "latest",
                key => return Err(anyhow!("{} config does not support option {}", KAFKA_CONFIG_SCAN_STARTUP_MODE, key)),
            }
        };

        // disable partition eof
        config.set("enable.partition.eof", "false");
        config.set("enable.auto.commit", "false");
        config.set("auto.offset.reset", auto_offset_reset);
        config.set("bootstrap.servers", broker);

        config.set("group.id", properties.get(KAFKA_CONFIG_CONSUME_GROUP).ok().unwrap_or(format!(
            "consumer-{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros()) as String));

        let consumer = config
            .set_log_level(RDKafkaLogLevel::Info)
            .create_with_context(DefaultConsumerContext)
            .map_err(|e| anyhow!("consumer creation failed {}", e.to_string()))?;

        Ok(Self {
            consumer: Arc::new(consumer),
        })
    }
}
