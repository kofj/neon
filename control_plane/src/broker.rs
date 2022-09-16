use std::{
    fs,
    path::PathBuf,
    process::{Command, Stdio},
};

use anyhow::Context;
use nix::{
    sys::signal::{kill, Signal},
    unistd::Pid,
};

use crate::{local_env, read_pidfile};

pub fn start_broker_process(env: &local_env::LocalEnv) -> anyhow::Result<()> {
    let broker = &env.broker;

    let log_file_path = env.base_data_dir.join("neon_broker.log");

    let out_file = fs::File::create(log_file_path.clone()).with_context(|| {
        format!(
            "Failed to create neon_broker out file at {}",
            log_file_path.display()
        )
    })?;
    let listen_addr = &broker.listen_addr;

    println!("Starting neon broker at {}", listen_addr);

    let neon_broker_process = Command::new(env.neon_broker_bin())
        .args(&[format!("--listen-addr={listen_addr}")])
        .stdout(Stdio::from(out_file.try_clone()?))
        .stderr(Stdio::from(out_file))
        .spawn()
        .context("Failed to spawn neon broker subprocess")?;
    let pid = neon_broker_process.id();

    let neon_broker_pid_file_path = neon_broker_pid_file_path(env);
    fs::write(&neon_broker_pid_file_path, pid.to_string()).with_context(|| {
        format!(
            "Failed to create neon broker pid file at {}",
            neon_broker_pid_file_path.display()
        )
    })?;

    Ok(())
}

pub fn stop_broker_process(env: &local_env::LocalEnv) -> anyhow::Result<()> {
    println!("Stopping neon broker");

    let neon_broker_pid_file_path = neon_broker_pid_file_path(env);
    let pid = Pid::from_raw(read_pidfile(&neon_broker_pid_file_path).with_context(|| {
        format!(
            "Failed to read neon broker pid file at {}",
            neon_broker_pid_file_path.display()
        )
    })?);

    kill(pid, Signal::SIGTERM).with_context(|| {
        format!(
            "Failed to stop neon broker with pid {pid} at {}",
            neon_broker_pid_file_path.display()
        )
    })?;

    Ok(())
}

fn neon_broker_pid_file_path(env: &local_env::LocalEnv) -> PathBuf {
    env.base_data_dir.join("neon_broker.pid")
}
