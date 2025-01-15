use std::num::NonZeroUsize;

use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[derive(Default)]
pub struct RuntimeOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub server_runtime_threads: Option<NonZeroUsize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exec_runtime_threads: Option<NonZeroUsize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub io_runtime_threads: Option<NonZeroUsize>,
}
