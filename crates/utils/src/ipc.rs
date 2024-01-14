/*
 * Copyright (c) 2023 Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart Mail Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use std::{borrow::Cow, path::PathBuf, pin::Pin};

use bytes::{Bytes, BytesMut};
use futures::Stream;
use tokio::{fs, io::AsyncReadExt, sync::oneshot};

#[derive(Debug)]
pub enum DeliveryEvent {
    Ingest {
        message: IngestMessage,
        result_tx: oneshot::Sender<Vec<DeliveryResult>>,
    },
    Stop,
}

#[derive(Debug)]
pub struct IngestMessage {
    pub sender_address: String,
    pub recipients: Vec<String>,
    pub message_data: MessageData,
}

pub type BoxedError = Box<dyn std::error::Error + Sync + Send>;
pub type BoxedByteStream = Pin<Box<dyn Stream<Item = Result<Bytes, BoxedError>> + Send + 'static>>;

pub enum MessageData {
    File {
        message_path: PathBuf,
        message_size: usize,
    },
    Bytes(BoxedByteStream),
    Empty,
}

impl std::fmt::Debug for MessageData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File {
                message_path,
                message_size,
            } => f
                .debug_struct("File")
                .field("message_path", message_path)
                .field("message_size", message_size)
                .finish(),
            Self::Bytes(_) => f.debug_tuple("Bytes").finish(),
            Self::Empty => write!(f, "Empty"),
        }
    }
}

#[derive(Debug, Clone)]
pub enum DeliveryResult {
    Success,
    TemporaryFailure {
        reason: Cow<'static, str>,
    },
    PermanentFailure {
        code: [u8; 3],
        reason: Cow<'static, str>,
    },
}

impl IngestMessage {
    pub fn read_message(&mut self) -> BoxedByteStream {
        self.message_data.read_message()
    }
}

impl MessageData {
    pub fn read_message(&mut self) -> BoxedByteStream {
        match std::mem::replace(self, MessageData::Empty) {
            MessageData::File {
                message_path,
                message_size,
            } => Box::pin(futures::stream::once(async move {
                let mut raw_message = BytesMut::with_capacity(message_size);
                raw_message.resize(message_size, 0);
                let mut file = fs::File::open(&message_path).await.map_err(|err| {
                    tracing::error!(
                        context = "read_message",
                        event = "error",
                        "Failed to open message file {}: {}",
                        message_path.display(),
                        err
                    );
                    err
                })?;
                file.read_exact(&mut raw_message).await.map_err(|err| {
                    tracing::error!(
                        context = "read_message",
                        event = "error",
                        "Failed to read {} bytes file {} from disk: {}",
                        message_size,
                        message_path.display(),
                        err
                    );
                    err
                })?;
                Ok(raw_message.freeze())
            })),
            MessageData::Bytes(b) => b,
            MessageData::Empty => Box::pin(futures::stream::empty()),
        }
    }
}
