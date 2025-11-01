use std::pin::Pin;
use runtime::{PSLRuntime, RequestHandler};

#[no_mangle]
pub extern "C" fn hello_world(
    request: *const runtime::RequestBody,
    response: *mut runtime::ResponseBody,
) -> i32 {
    let request = unsafe { &*request };
    let response = unsafe { &mut *response };

    // Set the response status and body
    response.status = 200;
    response.body = "Hello, World!";

    200
}


#[no_mangle]
pub extern "C" fn setup() -> Pin<Box<PSLRuntime<'static>>> {
    let mut runtime = PSLRuntime::new();

    // Register the request handlers
    runtime.num_handlers = 1;
    runtime.request_url_paths = &["/hello"];
    runtime.request_function_names = &["hello_world"];

    Box::pin(runtime)
}

#[no_mangle]
pub extern "C" fn hello() {
    println!("Hello from Rust!");
}