#[derive(Clone, Debug)]
enum Cloud {
    Aws,
    Azure,
    Gcp,
    Other(String),
}

#[derive(Clone, Debug)]
struct Region {
    cloud: Option<Cloud>,
    name: String,
}

#[derive(Clone, Debug)]
struct Zone {
    region: Region,
    name: String,
}
