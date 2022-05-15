use std::fmt::Display;
use chrono::Utc;
use clap::Parser;
use config::Config;
use futures::try_join;
use serde::Deserialize;

#[derive(Parser, Debug)]
struct Args {
    #[clap(short, long, default_value = "GBP")]
    currency: String,
    #[clap(short, long, default_value_t = 30)]
    interval: u64,
    #[clap(long)]
    config: String,
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    data: Price
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
    Spot
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
        write!(f, "{},source={},currency={} buy={},sell={},spot={} {}", self.asset, self.source, self.currency, self.buy, self.sell, self.spot, self.timestamp.timestamp())
    }
}

impl Display for PriceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", match self {
            PriceType::Buy => "buy",
            PriceType::Sell => "sell",
            PriceType::Spot => "spot"
        })
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
    let start = tokio::time::Instant::now();
    let mut interval = tokio::time::interval_at(start, tokio::time::Duration::from_secs(args.interval));
    loop {
        interval.tick().await;
        let (buy, sell, spot) = try_join!(
            get_price(PriceType::Buy, &args.currency),
            get_price(PriceType::Sell, &args.currency),
            get_price(PriceType::Spot, &args.currency)
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
        submit_influx(&influx_config, &pd).await?;
    }
}

async fn get_price(typ: PriceType, currency: &str) -> Result<Price, Box<dyn std::error::Error>> {
    let url = format!("https://api.coinbase.com/v2/prices/{}?currency={}", typ, currency);
    let response = reqwest::get(url).await?.json::<ApiResponse>().await?;
    Ok(response.data)
}

async fn submit_influx(config: &InfluxConfig, price_data: &PriceData) -> Result<(), Box<dyn std::error::Error>> {
    let uri = format!("{}/api/v2/write?bucket={}&org={}&precision=s", config.host, config.bucket, config.org);
    let client = reqwest::Client::new();
    let response = client
        .post(uri)
        .header("Authorization", format!("Token {}", config.token))
        .header("Accept", "application/json")
        .header("Content-Type", "text/plain; charset=utf-8")
        .body(format!("{}", price_data))
        .send()
        .await?;
    if response.status() != 204 {
        println!("incorrect status");
        panic!();
    }
    Ok(())
}
