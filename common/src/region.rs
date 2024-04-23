#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub enum Cloud {
    Aws,
    Azure,
    Gcp,
    Other(String),
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct Region {
    cloud: Option<Cloud>,
    name: String,
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash)]
pub struct Zone {
    pub region: Region,
    pub name: String,
}
