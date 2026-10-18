//! The JS side the engine calls into: session events, logging, panics.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use std::future::Future;
use std::pin::Pin;

use mfcore::pipeline::capture::CaptureSink;
use mfcore::platform::SessionObserver;
use mfcore::util::http::{HeaderRuleHost, HeaderRuleId};
use mfcore::{AnalysisSession, LiveSession, SessionEvent};
use send_wrapper::SendWrapper;
use wasm_bindgen::prelude::*;

use crate::convert::to_js;

/// Sessions the facade can finish or cancel by id, until they end. These are
/// the only strong references: the handles JS holds are weak, so a handle the
/// page never frees cannot keep the library open after its session ended.
#[derive(Default)]
pub struct Sessions {
    pub live: RefCell<HashMap<String, Arc<LiveSession>>>,
    pub analyses: RefCell<HashMap<String, Arc<AnalysisSession>>>,
    pub captures: RefCell<HashMap<String, Arc<CaptureSink>>>,
}

impl Sessions {
    fn ended(&self, id: &str) {
        self.live.borrow_mut().remove(id);
        self.analyses.borrow_mut().remove(id);
        self.captures.borrow_mut().remove(id);
    }

    pub fn clear(&self) {
        self.live.borrow_mut().clear();
        self.analyses.borrow_mut().clear();
        self.captures.borrow_mut().clear();
    }
}

/// `host.onEvent(sessionId, event)` for one session. The id comes with the
/// session's first event (`Started`), which the engine always sends first.
pub struct JsObserver {
    on_event: SendWrapper<js_sys::Function>,
    id: SendWrapper<RefCell<Option<String>>>,
    sessions: SendWrapper<Rc<Sessions>>,
}

impl JsObserver {
    pub fn new(on_event: js_sys::Function, sessions: Rc<Sessions>) -> Arc<Self> {
        Arc::new(Self {
            on_event: SendWrapper::new(on_event),
            id: SendWrapper::new(RefCell::new(None)),
            sessions: SendWrapper::new(sessions),
        })
    }
}

impl SessionObserver for JsObserver {
    fn on_event(&self, event: SessionEvent) {
        if let SessionEvent::Started { session_id, .. } = &event {
            *self.id.borrow_mut() = Some(session_id.clone());
        }
        let Some(id) = self.id.borrow().clone() else {
            tracing::error!("an event arrived before Started: {event:?}");
            return;
        };
        let terminal =
            matches!(event, SessionEvent::Finished { .. } | SessionEvent::Failed { .. } | SessionEvent::Canceled);
        if terminal {
            self.sessions.ended(&id);
        }
        match to_js(&event) {
            Ok(value) => {
                if let Err(e) = self.on_event.call2(&JsValue::NULL, &JsValue::from_str(&id), &value) {
                    web_sys::console::error_2(&"Music Fetch: the event listener failed".into(), &e);
                }
            }
            Err(e) => web_sys::console::error_2(&"Music Fetch: an event could not be passed on".into(), &e),
        }
    }
}

/// `host.prepare(rule, variant)`: installs a header rule variant (the Shazam agent
/// for the next request) in the extension's declarativeNetRequest rules and
/// resolves once it is in place.
pub struct JsRuleHost {
    prepare: SendWrapper<js_sys::Function>,
}

impl JsRuleHost {
    pub fn new(prepare: js_sys::Function) -> Arc<Self> {
        Arc::new(Self { prepare: SendWrapper::new(prepare) })
    }
}

impl HeaderRuleHost for JsRuleHost {
    fn prepare(&self, rule: HeaderRuleId, variant: usize) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        let name = serde_json::to_value(rule).ok().and_then(|v| v.as_str().map(str::to_string)).unwrap_or_default();
        let result = self.prepare.call2(&JsValue::NULL, &JsValue::from_str(&name), &JsValue::from(variant as u32));
        Box::pin(SendWrapper::new(async move {
            match result {
                Ok(value) => {
                    if let Ok(promise) = value.dyn_into::<js_sys::Promise>()
                        && let Err(e) = wasm_bindgen_futures::JsFuture::from(promise).await
                    {
                        web_sys::console::warn_2(&"Music Fetch: a header rule was not installed".into(), &e);
                    }
                }
                Err(e) => web_sys::console::warn_2(&"Music Fetch: a header rule was not installed".into(), &e),
            }
        }))
    }
}

/// Panics print to the console before the instance aborts (the host then
/// restarts the Worker; the next start marks the running sessions failed).
pub fn install_panic_hook() {
    use std::sync::Once;
    static HOOK: Once = Once::new();
    HOOK.call_once(|| {
        std::panic::set_hook(Box::new(|info| {
            web_sys::console::error_1(&format!("Music Fetch engine panicked: {info}").into());
        }));
    });
}

/// `tracing` events at `max` and above go to the console.
pub fn install_logging(max: tracing::Level) {
    let _ = tracing::subscriber::set_global_default(Console { max });
}

struct Console {
    max: tracing::Level,
}

impl tracing::Subscriber for Console {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        *metadata.level() <= self.max
    }

    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut line = Line::default();
        event.record(&mut line);
        let meta = event.metadata();
        let text = format!("[mfcore {}] {}{}", meta.target(), line.message, line.fields);
        let text = JsValue::from_str(&text);
        match *meta.level() {
            tracing::Level::ERROR => web_sys::console::error_1(&text),
            tracing::Level::WARN => web_sys::console::warn_1(&text),
            tracing::Level::INFO => web_sys::console::info_1(&text),
            _ => web_sys::console::debug_1(&text),
        }
    }

    fn enter(&self, _: &tracing::span::Id) {}

    fn exit(&self, _: &tracing::span::Id) {}
}

#[derive(Default)]
struct Line {
    message: String,
    fields: String,
}

impl tracing::field::Visit for Line {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else {
            self.fields.push_str(&format!(" {}={value:?}", field.name()));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields.push_str(&format!(" {}={value}", field.name()));
        }
    }
}
