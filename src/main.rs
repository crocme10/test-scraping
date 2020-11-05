use clap::{App, Arg, ArgMatches, SubCommand};
use log::{error, trace, warn};
use scraper::{Html, Selector};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Error, ErrorKind, Write};
use tokio::fs;

#[derive(Debug, Serialize, Deserialize)]
pub struct Character {
    pub name: String,
    pub portrayal: String,
    pub description: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    pretty_env_logger::init();
    let matches = App::new("Searching Star Wars Characters with Elasticsearch")
        .version("0.1")
        .author("Matthieu Paindavoine <matt@area403.org>")
        .subcommand(
            SubCommand::with_name("index")
                .about("Scrap data and index them in Elasticsearch")
                .version("0.1")
                .author("Matthieu Paindavoine <matt@area403.org>"),
        )
        .subcommand(SubCommand::with_name("init").about("Initialize Elasticsearch"))
        .subcommand(
            SubCommand::with_name("search").about("Search").arg(
                Arg::with_name("query")
                    .value_name("FEATURE")
                    .help("Feature to search for"),
            ),
        )
        .get_matches();

    match matches.subcommand() {
        ("index", Some(sm)) => index(sm).await,
        ("init", Some(sm)) => init(sm).await,
        ("search", Some(sm)) => search(sm).await,
        _ => {
            warn!("Unrecognized subcommand");
            Err(String::from("foo").into())
            // Err(Box::new(Error::new(
            //     ErrorKind::Other,
            //     "Unrecognized subcommand",
            // )))
        }
    }
}

async fn index<'a>(_matches: &ArgMatches<'a>) -> Result<(), Box<dyn std::error::Error>> {
    create_index("starwars").await?;

    generate_dataset("https://en.wikipedia.org/wiki/List_of_Star_Wars_characters").await?;

    generate_bulk_input("starwars").await?;

    import_bulk_input("starwars").await?;

    Ok(())
}

async fn init<'a>(_matches: &ArgMatches<'a>) -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}

async fn search<'a>(matches: &ArgMatches<'a>) -> Result<(), Box<dyn std::error::Error>> {
    let query = matches.value_of("query").expect("Query parameter");

    let res = search_query("starwars", query).await?;

    println!("{}", res.to_string());

    Ok(())
}

async fn generate_dataset(url: &str) -> Result<(), Box<dyn std::error::Error>> {
    trace!("Creating dataset from {}", url);
    let body = reqwest::get(url).await?.text().await?;

    let fragment = Html::parse_document(&body);

    let rows_selector = Selector::parse("table.wikitable > tbody > tr").unwrap();
    let cells_selector = Selector::parse("td").unwrap();

    // iterate over elements matching our selector
    let characters = fragment
        .select(&rows_selector)
        .map(|row| {
            let dat = row
                .select(&cells_selector)
                .map(|cell| cell.text().collect::<Vec<_>>().join(""))
                .map(|mut t| {
                    t.pop(); // remove trailing \n
                    t
                })
                .collect::<Vec<_>>();

            if dat.len() == 3 {
                Ok(Character {
                    name: dat[0].clone(),
                    portrayal: dat[1].clone(),
                    description: dat[2].clone(),
                })
            } else {
                Err(Error::new(ErrorKind::Other, "oh no!"))
            }
        })
        .filter_map(|rc| rc.ok())
        .collect::<Vec<_>>();

    trace!("Writing dataset to 'dataset.json'");
    let mut file = std::fs::File::create("dataset.json")?;

    serde_json::to_writer_pretty(&mut file, &characters)?;
    trace!("Dataset dataset.json successfully created");

    Ok(())
}

async fn create_index(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = format!("http://localhost:9200/{}", name);
    trace!("Creating index {}", endpoint);
    let contents = fs::read_to_string("settings.json").await?;
    let settings: Value = serde_json::from_str(&contents)?;
    let client = reqwest::Client::new();
    let resp = client
        .put(&endpoint)
        .header("Content-Type", "application/json")
        .json(&settings)
        .send()
        .await?;
    if resp.status().is_success() {
        trace!("Index {} successfully created", endpoint);
        Ok(())
    } else {
        let resp_status = String::from(resp.status().as_str());
        let resp_msg = resp.text().await.expect("Response");
        error!(
            "Index '{}' creation failed with status {}: {}",
            name, resp_status, resp_msg
        );
        Err(Box::new(Error::new(
            ErrorKind::Other,
            format!("Index '{}' failure: status {}", name, resp_status),
        )))
    }
}

async fn generate_bulk_input(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    trace!("Creating bulk input 'bulk.json'");
    let mut file = std::fs::File::create("bulk.json")?;
    let contents = fs::read_to_string("dataset.json").await?;
    let value: Value = serde_json::from_str(&contents)?;
    let values: &Vec<Value> = value.as_array().expect("dataset should be a JSON array");
    values.iter().for_each(|value| {
        let id = uuid::Uuid::new_v4();
        let json = format!(
            "{{ \"index\": {{ \"_index\": \"{}\", \"_type\": \"_doc\", \"_id\": \"{}\" }} }}\n",
            name, id
        );
        file.write_all(json.as_bytes()).unwrap();
        serde_json::to_writer(&mut file, &value).expect("could not write bulk");
        file.write_all("\n".as_bytes()).unwrap();
    });
    trace!("Bulk input 'bulk.json' successfully created");
    Ok(())
}

async fn import_bulk_input(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let endpoint = format!("http://localhost:9200/{}/_doc/_bulk", name);
    trace!("Importing bulk dataset to {}", endpoint);
    let file = std::fs::File::open("bulk.json").expect("no such file");
    let buf = BufReader::new(file);
    let chunks = buf
        .lines()
        .map(|l| {
            l.map(|mut z| {
                z.push('\n');
                z
            })
        })
        .collect::<Vec<Result<String, _>>>();
    let stream = futures::stream::iter(chunks);
    let body = reqwest::Body::wrap_stream(stream);
    let client = reqwest::Client::new();
    let resp = client
        .put(&endpoint)
        .body(body)
        .header("Content-Type", "application/json")
        .send()
        .await?;
    if resp.status().is_success() {
        trace!("Dataset successfully imported");
        Ok(())
    } else {
        let resp_status = String::from(resp.status().as_str());
        let resp_msg = resp.text().await.expect("Response");
        error!(
            "Bulk import {} failed with status {}: {}",
            name, resp_status, resp_msg
        );
        Err(Box::new(Error::new(
            ErrorKind::Other,
            format!("Bulk import {} failure: status {}", name, resp_status),
        )))
    }
}

async fn search_query(name: &str, query: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let endpoint = format!("http://localhost:9200/{}/_search", name);
    trace!("Searching endpoint {}", endpoint);
    let json = build_query(query)?;
    let client = reqwest::Client::new();
    let resp = client.get(&endpoint).json(&json).send().await?;
    if resp.status().is_success() {
        trace!("Dataset successfulyl searched");
        let ret = resp.json::<Value>().await?;
        Ok(ret)
    } else {
        let resp_status = String::from(resp.status().as_str());
        let resp_msg = resp.text().await.expect("Response");
        error!(
            "Dataset search failed with status {}: {}",
            resp_status, resp_msg
        );
        Err(Box::new(Error::new(
            ErrorKind::Other,
            format!("Dataset search failed: status {}: {}", name, resp_status),
        )))
    }
}
fn build_query(query: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let json = json!({
        "query": {
            "multi_match": {
                "query": query,
                "fields": [
                    "name^10",
                    "description"
                ]
            }
        }
    });
    Ok(json)
}
