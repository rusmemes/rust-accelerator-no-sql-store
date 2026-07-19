use super::*;
use crate::manager::service::test_support::*;

#[tokio::test]
async fn init_with_manager_connection_requests_connection_and_sets_state() {
    let me = me("11111111-1111-1111-1111-111111111111");
    let (mut service, _config) = service(me.clone());

    let output = service.get_init_messages().await;

    assert!(matches!(
        output.as_slice(),
        [ManagerProtocol::NewConnection {
            id: _,
            host,
            port,
            manager: true,
        }] if host == "manager.local" && *port == 9000
    ));

    let state = service.state.as_ref().expect("state initialized");
    assert_eq!(state.epoch, None);
    assert_eq!(state.elected_leader_id, None);
    assert!(state.nodes.contains_key(&me.id));
}
