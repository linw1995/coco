use std::io::{Read, Write};
use std::os::unix::net::UnixListener;
use std::process::{Command, Stdio};
use std::thread;

use coco_llm::{
    COCO_CLI_RUNTIME_SOCKET_ENV, COCO_PARENT_TOOL_USE_ID_ENV, COCO_SESSION_BRANCH_ENV,
    COCO_SESSION_ROLE_ENV, COCO_STORE_PATH_ENV, CocoCliRuntimeRequest, CocoCliRuntimeResponse,
};
use coco_mem::SessionRole;
use tempfile::tempdir;

#[test]
fn binary_forwards_prompt_stdin_to_runtime_socket() {
    let dir = tempdir().unwrap();
    let socket_path = dir.path().join("coco-runtime.sock");
    let listener = UnixListener::bind(&socket_path).unwrap();
    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut payload = Vec::new();
        stream.read_to_end(&mut payload).unwrap();
        let request = serde_json::from_slice::<CocoCliRuntimeRequest>(&payload).unwrap();
        let response = CocoCliRuntimeResponse {
            exit_code: 7,
            stdout: "runtime output\n".to_owned(),
            stderr: "runtime warning\n".to_owned(),
        };
        stream
            .write_all(&serde_json::to_vec(&response).unwrap())
            .unwrap();
        request
    });

    let mut child = Command::new(env!("CARGO_BIN_EXE_coco"))
        .env(COCO_CLI_RUNTIME_SOCKET_ENV, &socket_path)
        .env(COCO_SESSION_BRANCH_ENV, "feature")
        .env(COCO_SESSION_ROLE_ENV, "runner")
        .env(COCO_STORE_PATH_ENV, "/tmp/store")
        .env(COCO_PARENT_TOOL_USE_ID_ENV, "tool-call")
        .env("RUST_LOG", "warn")
        .args(["job", "--branch", "draft"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"prompt input")
        .unwrap();
    drop(child.stdin.take());

    let output = child.wait_with_output().unwrap();
    let request = server.join().unwrap();

    assert_eq!(output.status.code(), Some(7));
    assert_eq!(output.stdout, b"runtime output\n");
    assert_eq!(output.stderr, b"runtime warning\n");
    assert_eq!(request.args, ["job", "--branch", "draft"]);
    assert_eq!(request.stdin, b"prompt input");
    assert_eq!(request.branch_env.as_deref(), Some("feature"));
    assert_eq!(request.session_role, Some(SessionRole::Runner));
    assert_eq!(request.store_path_env.as_deref(), Some("/tmp/store"));
    assert_eq!(request.parent_tool_use_id_env.as_deref(), Some("tool-call"));
}
