use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = env!("CARGO_PKG_NAME"), version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = env!("CARGO_PKG_DESCRIPTION"))]
enum Opt {
    #[structopt(name = "get", about = "gets a value given a key")]
    Get {
        #[structopt(required = true, help = "Key")]
        key: String,
    },
    #[structopt(name = "set", about = "sets a value given a key")]
    Set {
        #[structopt(required = true, help = "Key")]
        key: String,
        #[structopt(required = true, help = "Value")]
        value: String,
    },
    #[structopt(name = "rm", about = "removes a value, given its key")]
    Rm {
        #[structopt(required = true, help = "Key")]
        key: String,
    },
}

fn main() {
    match Opt::from_args() {
        Opt::Get { key } => get(key),
        Opt::Set { key, value } => set(key, value),
        Opt::Rm { key } => rm(key),
    }
}

fn get(key: String) {
    panic!("unimplemented")
}

fn set(key: String, value: String) {
    panic!("unimplemented")
}

fn rm(key: String) {
    panic!("unimplemented")
}
