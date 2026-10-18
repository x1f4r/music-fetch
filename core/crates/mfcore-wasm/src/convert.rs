//! Values across the JS boundary: serde's default representation (snake_case
//! fields, externally tagged enums, `null` for `None`, plain objects for maps),
//! which is what `extension/src/engine/types.gen.ts` describes.

use mfcore::MfError;
use serde::Serialize;
use serde::de::DeserializeOwned;
use wasm_bindgen::prelude::*;

/// `value` as a plain JS value.
pub fn to_js<T: Serialize + ?Sized>(value: &T) -> Result<JsValue, JsValue> {
    let serializer =
        serde_wasm_bindgen::Serializer::new().serialize_missing_as_null(true).serialize_maps_as_objects(true);
    value.serialize(&serializer).map_err(|e| error(&MfError::internal(format!("cannot pass a value to JS: {e}"))))
}

/// A JS value as `T`; an `InvalidInput` error names `what` when it does not fit.
pub fn from_js<T: DeserializeOwned>(value: JsValue, what: &str) -> Result<T, JsValue> {
    serde_wasm_bindgen::from_value(value).map_err(|e| error(&MfError::invalid(format!("{what} is not valid: {e}"))))
}

/// A rejected call: an `Error` whose `code` is the core's `ErrorCode` name
/// (the extension turns it into an `EngineError`).
pub fn error(e: &MfError) -> JsValue {
    let message = match e {
        // The user-facing text without the category prefix.
        MfError::Network { message }
        | MfError::Decode { message }
        | MfError::Storage { message }
        | MfError::Internal { message } => message.clone(),
        other => other.to_string(),
    };
    let err = js_sys::Error::new(&message);
    let code = if matches!(e, MfError::Canceled) { "Canceled".to_string() } else { format!("{:?}", e.code()) };
    let _ = js_sys::Reflect::set(&err, &"code".into(), &code.into());
    err.into()
}

/// `Result` of a core call as a JS result.
pub fn js<T>(result: mfcore::error::Result<T>) -> Result<T, JsValue> {
    result.map_err(|e| error(&e))
}
