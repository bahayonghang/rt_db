use tiberius::{Client, Config, AuthMethod, Row};
use tokio::net::TcpStream;
use tokio_util::compat::{TokioAsyncWriteCompatExt, Compat};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let mut config = Config::new();
    config.host("localhost");
    config.port(1433);
    config.database("控制器数据库");
    config.authentication(AuthMethod::sql_server("sa", "ysdxdckj@666"));
    config.trust_cert();
    
    let tcp = TcpStream::connect(config.get_addr()).await?;
    let mut client = Client::connect(config, tcp.compat_write()).await?;
    
    println!("检查TagDatabase表结构:");
    let stream = client.query("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'TagDatabase'", &[]).await?;
    let rows = stream.into_first_result().await?;
    
    for row in rows {
        let column_name: &str = row.get(0).unwrap_or("");
        let data_type: &str = row.get(1).unwrap_or("");
        println!("列名: {}, 类型: {}", column_name, data_type);
    }
    
    println!("\n检查历史表结构:");
    let stream = client.query("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '历史表'", &[]).await?;
    let rows = stream.into_first_result().await?;
    
    for row in rows {
        let column_name: &str = row.get(0).unwrap_or("");
        let data_type: &str = row.get(1).unwrap_or("");
        println!("列名: {}, 类型: {}", column_name, data_type);
    }
    
    Ok(())
}