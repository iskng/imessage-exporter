#[cfg(test)]
mod tests {
    use crate::{
        message_types::variants::{CustomBalloon, Variant},
        tables::messages::Message,
    };

    #[test]
    fn can_get_no_balloon_bundle_id() {
        let m = Message::blank();
        assert_eq!(m.parse_balloon_bundle_id(), None);
    }

    #[test]
    fn can_get_balloon_bundle_id_os() {
        let mut m = Message::blank();
        m.balloon_bundle_id = Some("com.apple.Handwriting.HandwritingProvider".to_owned());
        assert_eq!(
            m.parse_balloon_bundle_id(),
            Some("com.apple.Handwriting.HandwritingProvider")
        );
    }

    #[test]
    fn can_get_balloon_bundle_id_url() {
        let mut m = Message::blank();
        m.balloon_bundle_id = Some("com.apple.messages.URLBalloonProvider".to_owned());
        assert_eq!(
            m.parse_balloon_bundle_id(),
            Some("com.apple.messages.URLBalloonProvider")
        );
    }

    #[test]
    fn can_get_balloon_bundle_id_apple() {
        let mut m = Message::blank();
        m.balloon_bundle_id = Some("com.apple.messages.MSMessageExtensionBalloonPlugin:0000000000:com.apple.PassbookUIService.PeerPaymentMessagesExtension".to_owned());
        assert_eq!(
            m.parse_balloon_bundle_id(),
            Some("com.apple.PassbookUIService.PeerPaymentMessagesExtension")
        );
    }

    #[test]
    fn can_get_balloon_bundle_id_third_party() {
        let mut m = Message::blank();
        m.balloon_bundle_id = Some("com.apple.messages.MSMessageExtensionBalloonPlugin:QPU8QS3E62:com.contextoptional.OpenTable.Messages".to_owned());
        assert_eq!(
            m.parse_balloon_bundle_id(),
            Some("com.contextoptional.OpenTable.Messages")
        );
        assert!(matches!(
            m.variant(),
            Variant::App(CustomBalloon::Application(
                "com.contextoptional.OpenTable.Messages"
            ))
        ));
    }
}
