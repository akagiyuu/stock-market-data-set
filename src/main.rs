use anyhow::Result;
use chrono::NaiveDate;
use clap::Parser;
use futures::{stream::FuturesUnordered, TryStreamExt};
use scraper::{ElementRef, Html, Selector};
use std::{fmt::Display, path::PathBuf};
use tokio::{fs, io::AsyncWriteExt};
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

const HEADERS: &[u8] = b"Date,Open,High,Low,Close,Adj Close,Volume\n";

#[derive(Debug)]
struct Entry {
    date: NaiveDate,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    adj_close: f64,
    volume: u64,
}

impl Entry {
    fn from_element(row_elemnt: ElementRef<'_>) -> Result<Self> {
        let [date, open, high, low, close, adj_close, volume]: [_; 7] = row_elemnt
            .child_elements()
            .map(|element| element.inner_html())
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| anyhow::anyhow!("Failed to parse"))?;

        let date = NaiveDate::parse_from_str(&date, "%b %-d, %Y")?;
        let open = open.parse()?;
        let high = high.parse()?;
        let low = low.parse()?;
        let close = close.parse()?;
        let adj_close = adj_close.parse()?;
        let volume = volume.replace(',', "").parse()?;

        Ok(Entry {
            date,
            open,
            high,
            low,
            close,
            adj_close,
            volume,
        })
    }
}

impl Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},{},{},{},{},{},{}",
            self.date, self.open, self.high, self.low, self.close, self.adj_close, self.volume,
        )
    }
}

fn get_history_link(symbol: &str) -> String {
    format!(
        "https://finance.yahoo.com/quote/{}/history/?period1=345479400&period2=1722448703",
        symbol
    )
}

#[tracing::instrument(err, skip(output))]
async fn get_data(symbol: &str, output: PathBuf) -> Result<()> {
    let history_link = get_history_link(symbol);

    tracing::info!("Fetch html file containing data");

    let html = reqwest::Client::new()
        .get(history_link)
        .header(
            "User-Agent",
            "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
        )
        .send()
        .await?
        .text()
        .await?;

    tracing::info!("Finish fetch html file containing data");

    tracing::info!("Parse html file for entries");

    let html = Html::parse_fragment(&html);
    let selector = Selector::parse("tr.yf-ewueuo").unwrap();
    let data = html
        .select(&selector)
        .skip(1)
        .flat_map(Entry::from_element)
        .map(|x| x.to_string())
        .fold(String::with_capacity(100000), |mut data, x| {
            data.push_str(&x);
            data.push('\n');
            data
        });

    tracing::info!("Finish parse html file for entries");

    tracing::info!("Write all data to {:?}", output.as_os_str());
    let mut file = fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&output)
        .await?;

    file.write_all(HEADERS).await?;
    file.write_all(data.as_bytes()).await?;

    tracing::info!("Finish write all data to {:?}", output.as_os_str());

    Ok(())
}

#[derive(Debug, Parser)]
struct Args {
    #[arg(short, long, value_parser, num_args = 1.., value_delimiter = ' ')]
    symbols: Vec<String>,
    #[arg(id = "output-dir", short, long)]
    output_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().pretty())
        .with(
            EnvFilter::builder()
                .with_default_directive(LevelFilter::INFO.into())
                .from_env_lossy(),
        )
        .init();

    let args = Args::parse();
    let output_dir = args.output_dir.as_path();
    fs::create_dir_all(output_dir).await?;

    args.symbols
        .iter()
        .map(|symbol| {
            let output = output_dir.join(symbol).with_extension("csv");
            get_data(symbol, output)
        })
        .collect::<FuturesUnordered<_>>()
        .try_collect::<Vec<_>>()
        .await?;

    Ok(())
}
