use Batch

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Payload {
  c_type: usize,
  ciphertext: String,
}

impl Payload {
  pub fn new(c_type: usize, ciphertext: String) -> Payload {
    Self { c_type, ciphertext }
  }

  pub fn c_type(&self) -> usize {
    self.c_type
  }

  pub fn ciphertext(&self) -> &String {
    &self.ciphertext
  }
}

#[tokio::test]
async fn test_message_post() {
    let idkey = String::from("efgh");
    let payload = String::from("hello");
    let batch = Batch::from_vec(vec![OutgoingMessage::new(
        idkey.clone(),
        Payload::new(0, payload.clone())
    )]);

    // TODO: finish
}
