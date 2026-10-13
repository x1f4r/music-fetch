use crate::error::Result;
use crate::providers::ProviderMatch;
use crate::util::cancel::CancelToken;

pub struct AudD {
    _http: reqwest::Client,
    _token: String,
}

impl AudD {
    pub fn new(http: reqwest::Client, token: String) -> Self {
        Self { _http: http, _token: token }
    }

    /// Recognize a WAV clip (≤ 12 s).
    pub async fn recognize(&self, wav: Vec<u8>, cancel: &CancelToken) -> Result<Option<ProviderMatch>> {
        let _ = (wav, cancel);
        todo!("providers::audd")
    }
}
