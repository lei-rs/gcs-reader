pub use reader::{Auth, GCSReader};
pub use uri::GCSObjectURI;

mod errors;
mod reader;
mod uri;

#[cfg(test)]
use std::sync::Once;

#[cfg(test)]
static INIT: Once = Once::new();

#[cfg(test)]
pub(crate) fn setup() {
    INIT.call_once(|| {
        color_eyre::install().unwrap();
    });
}
