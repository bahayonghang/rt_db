//! 配置解析测试程序
//! 用于测试连接字符串解析和结构化配置的功能

use anyhow::Result;
use crate::config::{AppConfig, DatabaseConfig, DatabaseConnectionType};

/// 测试连接字符串解析功能
pub fn test_connection_string_parsing() -> Result<()> {
    println!("=== 测试连接字符串解析功能 ===");
    
    // 测试基本连接字符串
    let connection_string = "server=tcp:localhost,1433;database=TestDB;user=sa;password=123456;TrustServerCertificate=true";
    println!("\n测试连接字符串: {}", connection_string);
    
    match DatabaseConfig::from_connection_string(connection_string) {
        Ok(config) => {
            println!("解析成功:");
            println!("  服务器: {}", config.server);
            println!("  端口: {}", config.port);
            println!("  数据库: {}", config.database);
            println!("  用户名: {}", config.user);
            println!("  密码: {}", config.password);
            println!("  信任证书: {}", config.trust_server_certificate);
            
            // 测试重新生成连接字符串
            let regenerated = config.to_connection_string();
            println!("  重新生成的连接字符串: {}", regenerated);
        }
        Err(e) => {
            println!("解析失败: {}", e);
            return Err(e);
        }
    }
    
    // 测试包含中文的连接字符串
    let chinese_connection_string = "server=tcp:localhost,1433;database=%E6%8E%A7%E5%88%B6%E5%99%A8%E6%95%B0%E6%8D%AE%E5%BA%93;user=sa;password=test123;TrustServerCertificate=true";
    println!("\n测试中文数据库名连接字符串: {}", chinese_connection_string);
    
    match DatabaseConfig::from_connection_string(chinese_connection_string) {
        Ok(config) => {
            println!("解析成功:");
            println!("  服务器: {}", config.server);
            println!("  端口: {}", config.port);
            println!("  数据库: {}", config.database);
            println!("  用户名: {}", config.user);
            println!("  密码: {}", config.password);
            println!("  信任证书: {}", config.trust_server_certificate);
        }
        Err(e) => {
            println!("解析失败: {}", e);
            return Err(e);
        }
    }
    
    // 测试错误的连接字符串
    let invalid_connection_string = "invalid_format";
    println!("\n测试无效连接字符串: {}", invalid_connection_string);
    
    match DatabaseConfig::from_connection_string(invalid_connection_string) {
        Ok(_) => {
            println!("意外成功解析了无效连接字符串");
        }
        Err(e) => {
            println!("预期的解析失败: {}", e);
        }
    }
    
    Ok(())
}

/// 测试配置文件加载功能
pub fn test_config_loading() -> Result<()> {
    println!("\n=== 测试配置文件加载功能 ===");
    
    // 创建临时配置文件进行测试
    create_test_config_files()?;
    
    // 测试连接字符串模式
    println!("\n测试连接字符串模式:");
    match AppConfig::load("test_config_string.toml") {
        Ok(config) => {
            println!("配置加载成功:");
            println!("  连接方式: {:?}", config.database_connection_type);
            
            match config.get_database_config() {
                Ok(db_config) => {
                    println!("  数据库配置获取成功:");
                    println!("    服务器: {}", db_config.server);
                    println!("    数据库: {}", db_config.database);
                }
                Err(e) => {
                    println!("  数据库配置获取失败: {}", e);
                }
            }
        }
        Err(e) => {
            println!("配置加载失败: {}", e);
        }
    }
    
    // 测试结构化配置模式
    println!("\n测试结构化配置模式:");
    match AppConfig::load("test_config_struct.toml") {
        Ok(config) => {
            println!("配置加载成功:");
            println!("  连接方式: {:?}", config.database_connection_type);
            
            match config.get_database_config() {
                Ok(db_config) => {
                    println!("  数据库配置获取成功:");
                    println!("    服务器: {}", db_config.server);
                    println!("    数据库: {}", db_config.database);
                }
                Err(e) => {
                    println!("  数据库配置获取失败: {}", e);
                }
            }
        }
        Err(e) => {
            println!("配置加载失败: {}", e);
        }
    }
    
    // 清理测试文件
    cleanup_test_files();
    
    Ok(())
}

/// 创建测试配置文件
fn create_test_config_files() -> Result<()> {
    use std::fs;
    
    // 连接字符串模式配置
    let string_config = r#"
database_connection_type = "connection_string"
database_url = "server=tcp:localhost,1433;database=TestDB;user=sa;password=123456;TrustServerCertificate=true"

update_interval_secs = 30
data_window_days = 1
db_file_path = "./test.duckdb"
log_level = "info"

[tables]
history_table = "历史表"
tag_database_table = "TagDatabase"

[connection]
max_retries = 3
retry_interval_secs = 5
connection_timeout_secs = 30
"#;
    
    fs::write("test_config_string.toml", string_config)?;
    
    // 结构化配置模式配置
    let struct_config = r#"
database_connection_type = "structured_config"

update_interval_secs = 30
data_window_days = 1
db_file_path = "./test.duckdb"
log_level = "info"

[database]
server = "localhost"
port = 1433
database = "控制器数据库"
user = "sa"
password = "test123"
trust_server_certificate = true

[tables]
history_table = "历史表"
tag_database_table = "TagDatabase"

[connection]
max_retries = 3
retry_interval_secs = 5
connection_timeout_secs = 30
"#;
    
    fs::write("test_config_struct.toml", struct_config)?;
    
    Ok(())
}

/// 清理测试文件
fn cleanup_test_files() {
    use std::fs;
    
    let _ = fs::remove_file("test_config_string.toml");
    let _ = fs::remove_file("test_config_struct.toml");
}

/// 运行所有测试
pub fn run_all_tests() -> Result<()> {
    println!("开始配置解析功能测试...");
    
    test_connection_string_parsing()?;
    test_config_loading()?;
    
    println!("\n=== 所有测试完成 ===");
    Ok(())
}