use chrono::{Timelike, Utc};
use clap::Parser;
use config::Config;
use futures::try_join;
use reqwest::header::{HeaderMap, HeaderValue};
use serde::Deserialize;
use std::{fmt::Display, time::Duration};
use tokio::time::Instant;

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long, default_value = "GBP")]
    currency: String,
    #[clap(short, long, default_value_t = 30)]
    interval: u64,
    #[clap(long)]
    config: String,
    #[clap(long)]
    dry_run: bool,
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    data: Price,
}

#[derive(Debug, Deserialize)]
struct Price {
    // base: String,
    // currency: String,
    amount: String,
}

enum PriceType {
    Buy,
    Sell,
    Spot,
}

struct PriceData {
    source: String,
    asset: String,
    currency: String,
    buy: f32,
    sell: f32,
    spot: f32,
    timestamp: chrono::DateTime<Utc>,
}

#[derive(Debug)]
struct InfluxConfig {
    host: String,
    org: String,
    bucket: String,
    token: String,
}

impl Display for PriceData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{},source={},currency={} buy={},sell={},spot={} {}",
            self.asset,
            self.source,
            self.currency,
            self.buy,
            self.sell,
            self.spot,
            self.timestamp.timestamp()
        )
    }
}

impl Display for PriceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                PriceType::Buy => "buy",
                PriceType::Sell => "sell",
                PriceType::Spot => "spot",
            }
        )
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let config = Config::builder()
        .add_source(config::File::with_name(&args.config))
        .build()?;
    let influx_config = InfluxConfig {
        host: config.get("host")?,
        bucket: config.get("bucket")?,
        org: config.get("org")?,
        token: config.get("token")?,
    };

    let now = Utc::now();
    let seconds: u64 = now.second().into(); // current seconds
    let nanos = now.nanosecond();
    let wait = (args.interval - ((seconds + args.interval) % args.interval)) % 60;
    let start = Instant::now()
        .checked_add(Duration::new(wait - 1, 1_000_000_000 - nanos))
        .expect("Failed to set start");
    let mut interval = tokio::time::interval_at(start, Duration::from_secs(args.interval));

    let mut cb_default_headers = HeaderMap::new();
    cb_default_headers.insert("Accept", HeaderValue::from_static("application/json"));
    let cb_client = reqwest::Client::builder()
        .use_rustls_tls()
        .https_only(true)
        .default_headers(cb_default_headers)
        .gzip(true)
        .build()?;

    let mut if_default_headers = HeaderMap::new();
    if_default_headers.insert("Accept", HeaderValue::from_static("application/json"));
    if_default_headers.insert(
        "Content-Type",
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    if_default_headers.insert(
        "Authorization",
        HeaderValue::from_str(&format!("Token {}", influx_config.token))?,
    );
    let if_client = reqwest::Client::builder()
        .default_headers(if_default_headers)
        .gzip(true)
        .build()?;

    loop {
        interval.tick().await;
        let (buy, sell, spot) = try_join!(
            get_price(cb_client.clone(), PriceType::Buy, &args.currency),
            get_price(cb_client.clone(), PriceType::Sell, &args.currency),
            get_price(cb_client.clone(), PriceType::Spot, &args.currency)
        )?;
        let pd = PriceData {
            source: "Coinbase".into(),
            asset: "BTC".into(),
            currency: args.currency.clone(),
            buy: buy.amount.parse()?,
            sell: sell.amount.parse()?,
            spot: spot.amount.parse()?,
            timestamp: Utc::now(),
        };
        if !args.dry_run {
            submit_influx(if_client.clone(), &influx_config, &pd).await?;
        } else {
            println!("{}", pd);
        }
    }
}

async fn get_price(
    client: reqwest::Client,
    typ: PriceType,
    currency: &str,
) -> Result<Price, Box<dyn std::error::Error>> {
    let url = format!(
        "https://api.coinbase.com/v2/prices/{}?currency={}",
        typ, currency
    );
    let response = client.get(url).send().await?;
    let body = response.json::<ApiResponse>().await?;
    Ok(body.data)
}

async fn submit_influx(
    client: reqwest::Client,
    config: &InfluxConfig,
    price_data: &PriceData,
) -> Result<(), Box<dyn std::error::Error>> {
    let uri = format!(
        "{}/api/v2/write?bucket={}&org={}&precision=s",
        config.host, config.bucket, config.org
    );
    let response = client
        .post(uri)
        .body(format!("{}", price_data))
        .send()
        .await?;
    if response.status() != 204 {
        println!("incorrect status");
        panic!();
    }
    Ok(())
}
