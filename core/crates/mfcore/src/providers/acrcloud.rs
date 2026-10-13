use crate::config::AcrCredentials;
use crate::error::Result;
use crate::providers::ProviderMatch;
use crate::util::cancel::CancelToken;

pub struct AcrCloud {
    _http: reqwest::Client,
    _creds: AcrCredentials,
}

impl AcrCloud {
    pub fn new(http: reqwest::Client, creds: AcrCredentials) -> Self {
        Self { _http: http, _creds: creds }
    }

    /// Fingerprint recognition of a WAV clip (≤ 12 s).
    pub async fn recognize(&self, wav: Vec<u8>, cancel: &CancelToken) -> Result<Option<ProviderMatch>> {
        let _ = (wav, cancel);
        todo!("providers::acrcloud")
    }

    /// Humming recognition (project with the humming engine). Ranked, best first.
    pub async fn recognize_humming(&self, wav: Vec<u8>, cancel: &CancelToken) -> Result<Vec<ProviderMatch>> {
        let _ = (wav, cancel);
        todo!("providers::acrcloud")
    }
}
