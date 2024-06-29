#![deny(warnings)]

use datafusion_python::{context::PySessionContext, datafusion::prelude::SessionContext};
use pyo3::prelude::*;

/// Get datafusion session context
pub fn get_rust_context() -> SessionContext {
    SessionContext::new()
}

/// Get datafusion session context
#[pyfunction]
fn get_py_context() -> PySessionContext {
    let context = get_rust_context();
    PySessionContext::from(context)
}

#[pymodule]
fn my_library(_: Python<'_>, main_module: &PyModule) -> PyResult<()> {
    main_module.add_function(wrap_pyfunction!(get_py_context, main_module)?)?;
    Ok(())
}
