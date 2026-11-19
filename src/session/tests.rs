use super::{
    ConfirmedSession, InMemorySessionAuthorizer, SessionAuthorizationError, SessionAuthorizer,
    VerifiedPrincipal,
};

#[tokio::test]
async fn in_memory_grants_match_verified_subject_and_session() {
    let authorizer = InMemorySessionAuthorizer::new();
    let confirmed =
        ConfirmedSession::new("session-1", "driver-1", "brand-1").expect("valid confirmed IDs");
    authorizer
        .provision_confirmed_session("subject-1", confirmed.clone())
        .unwrap();

    assert_eq!(
        authorizer
            .resolve(
                &VerifiedPrincipal {
                    subject: "subject-1".into()
                },
                "session-1"
            )
            .await
            .unwrap(),
        confirmed
    );
    assert_eq!(
        authorizer
            .resolve(
                &VerifiedPrincipal {
                    subject: "other".into()
                },
                "session-1"
            )
            .await,
        Err(SessionAuthorizationError::Denied)
    );
    assert_eq!(
        authorizer
            .resolve(
                &VerifiedPrincipal {
                    subject: "subject-1".into()
                },
                "unknown"
            )
            .await,
        Err(SessionAuthorizationError::Denied)
    );
}
