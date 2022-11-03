use crate::{
    task_mgr::{self, spawn, TaskKind, BACKGROUND_RUNTIME},
    tenant::{Tenant, TenantState},
};
use std::{
    ops::Deref,
    sync::{Arc, Weak},
};
use tokio::sync::watch;
use tracing::{debug, error};

struct TenantGuardInner {
    tenant: Option<Tenant>,
    drop_watcher: watch::Sender<()>,
}

impl Drop for TenantGuardInner {
    fn drop(&mut self) {
        let tenant = self.tenant.take().unwrap();
        let tenant_id = tenant.tenant_id();
        let future = async move {
            debug!("shutdown tenant {tenant_id}");

            // Shut down all existing walreceiver connections and stop accepting the new ones.
            task_mgr::shutdown_tasks(Some(TaskKind::WalReceiverManager), None, None).await;

            // Ok, no background tasks running anymore. Flush any remaining data in
            // memory to disk.
            //
            // We assume that any incoming connections that might request pages from
            // the tenant have already been terminated by the caller, so there
            // should be no more activity in any of the repositories.
            //
            // On error, log it but continue with the shutdown for other tenants.
            if let Err(err) = tenant.checkpoint() {
                error!("Could not checkpoint tenant {tenant_id} during shutdown: {err:?}");
            }

            Ok(())
        };

        spawn(
            BACKGROUND_RUNTIME.handle(),
            TaskKind::TenantDrop,
            Some(tenant_id),
            None,
            "tenant drop",
            false,
            future,
        );

        self.drop_watcher.send_replace(());
    }
}

#[derive(Clone)]
pub struct TenantGuard(Weak<TenantGuardInner>);

impl TenantGuard {
    pub fn new(tenant: Tenant) -> (Self, AliveTenantGuard) {
        let mut recv = tenant.subscribe_for_state_updates();
        let (drop_watcher, _) = watch::channel(());
        let loop_arc = Arc::new(TenantGuardInner {
            tenant: Some(tenant),
            drop_watcher,
        });
        let arc_inner = loop_arc.clone();
        let weak_inner = Arc::downgrade(&loop_arc);

        let future = async move {
            while recv.changed().await.is_ok() {
                match *recv.borrow_and_update() {
                    TenantState::Active { .. } | TenantState::Paused => continue,
                    TenantState::Broken | TenantState::Stopping => break,
                }
            }
            drop(loop_arc)
        };
        BACKGROUND_RUNTIME.spawn(future);

        (Self(weak_inner), AliveTenantGuard(arc_inner))
    }

    pub fn upgrade(&self) -> Option<AliveTenantGuard> {
        let tenant = self.0.upgrade()?;
        match tenant.tenant.as_ref().unwrap().current_state() {
            TenantState::Active { .. } | TenantState::Paused => Some(AliveTenantGuard(tenant)),
            TenantState::Broken | TenantState::Stopping => None,
        }
    }

    pub fn current_state(&self) -> TenantState {
        match self.upgrade() {
            Some(tenant) => tenant.current_state(),
            None => TenantState::Paused,
        }
    }

    pub fn is_active(&self) -> bool {
        self.upgrade().is_some()
    }

    pub fn subscribe_for_tenant_shutdown(&self) -> Option<watch::Receiver<()>> {
        Some(self.upgrade()?.0.drop_watcher.subscribe())
    }
}

#[derive(Clone)]
pub struct AliveTenantGuard(Arc<TenantGuardInner>);

impl Deref for AliveTenantGuard {
    type Target = Tenant;

    fn deref(&self) -> &Self::Target {
        self.0.tenant.as_ref().unwrap()
    }
}
