#![deny(warnings)]

use datafusion::{arrow, error, logical_expr, prelude};
use pyo3::prelude::*;
use std::sync::Arc;

fn get_add_42_udf() -> logical_expr::ScalarUDF {
    let fun = Arc::new(
        |args: &[logical_expr::ColumnarValue]| -> error::Result<logical_expr::ColumnarValue> {
            match &args[0] {
                logical_expr::ColumnarValue::Array(array) => {
                    let input = array
                        .as_any()
                        .downcast_ref::<arrow::array::Float64Array>()
                        .unwrap();
                    println!("input: {:?}", input);
                    let result: arrow::array::Float64Array =
                        input.iter().map(|x| x.map(|x| x + 42.0)).collect();
                    println!("result: {:?}", result);
                    Ok(logical_expr::ColumnarValue::Array(Arc::new(result)))
                }
                _ => Err(error::DataFusionError::Internal(
                    "Invalid argument type".to_string(),
                )),
            }
        },
    );

    // Define the scalar UDF's implementation
    logical_expr::create_udf(
        "add_42",
        vec![arrow::datatypes::DataType::Float64],
        Arc::new(arrow::datatypes::DataType::Float64),
        logical_expr::Volatility::Immutable,
        fun,
    )
}

/// Get datafusion session context
pub fn get_rust_context() -> prelude::SessionContext {
    let ctx = prelude::SessionContext::new();
    let add_42 = get_add_42_udf();
    ctx.register_udf(add_42);
    ctx
}

/// Get datafusion session context
#[pyfunction]
fn get_py_context() -> datafusion_python::context::PySessionContext {
    let ctx = get_rust_context();
    datafusion_python::context::PySessionContext::from(ctx)
}

#[pymodule]
fn my_library(_: Python<'_>, module: Bound<'_, PyModule>) -> PyResult<()> {
    module.add_wrapped(wrap_pyfunction!(get_py_context))?;
    Ok(())
}
