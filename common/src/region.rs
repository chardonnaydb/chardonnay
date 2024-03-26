#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
pub enum Cloud {
    Aws,
    Azure,
    Gcp,
    Other(String),
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
pub struct Region {
    cloud: Option<Cloud>,
    name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
pub struct Zone {
    region: Region,
    name: String,
}
