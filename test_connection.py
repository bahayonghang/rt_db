#!/usr/bin/env python3
"""
测试脚本：验证实时数据缓存服务的功能

使用方法：
1. 确保服务正在运行
2. 运行此脚本：python test_connection.py
"""

import duckdb
import pandas as pd
from datetime import datetime, timedelta
import time
import os

def test_duckdb_connection():
    """测试DuckDB连接和数据查询"""
    db_file = "realtime_data.duckdb"
    
    if not os.path.exists(db_file):
        print(f"❌ 数据库文件不存在: {db_file}")
        print("请确保实时数据缓存服务正在运行")
        return False
    
    try:
        # 连接到数据库（只读模式）
        conn = duckdb.connect(db_file, read_only=True)
        print(f"✅ 成功连接到数据库: {db_file}")
        
        # 检查表是否存在
        tables = conn.execute("SHOW TABLES").fetchall()
        print(f"📋 数据库中的表: {[table[0] for table in tables]}")
        
        if ('ts_data',) not in tables:
            print("❌ ts_data 表不存在")
            return False
        
        # 获取总记录数
        total_count = conn.execute("SELECT COUNT(*) FROM ts_data").fetchone()[0]
        print(f"📊 总记录数: {total_count:,}")
        
        if total_count == 0:
            print("⚠️  数据库中没有数据")
            return True
        
        # 获取时间范围
        time_range = conn.execute(
            "SELECT MIN(ts) as min_time, MAX(ts) as max_time FROM ts_data"
        ).fetchone()
        print(f"⏰ 数据时间范围: {time_range[0]} 到 {time_range[1]}")
        
        # 获取标签统计
        tag_stats = conn.execute(
            "SELECT tag_name, COUNT(*) as count FROM ts_data GROUP BY tag_name ORDER BY count DESC LIMIT 10"
        ).fetchall()
        print("\n🏷️  前10个标签统计:")
        for tag, count in tag_stats:
            print(f"   {tag}: {count:,} 条记录")
        
        # 获取最近的数据
        recent_data = conn.execute(
            "SELECT * FROM ts_data ORDER BY ts DESC LIMIT 5"
        ).fetchdf()
        print("\n🕐 最近5条数据:")
        print(recent_data.to_string(index=False))
        
        # 测试时间范围查询
        one_hour_ago = datetime.now() - timedelta(hours=1)
        recent_count = conn.execute(
            "SELECT COUNT(*) FROM ts_data WHERE ts >= ?", 
            [one_hour_ago.strftime('%Y-%m-%d %H:%M:%S')]
        ).fetchone()[0]
        print(f"\n📈 最近1小时的数据: {recent_count:,} 条")
        
        conn.close()
        print("\n✅ 数据库测试完成")
        return True
        
    except Exception as e:
        print(f"❌ 数据库测试失败: {e}")
        return False

def monitor_data_updates(duration_minutes=5):
    """监控数据更新情况"""
    db_file = "realtime_data.duckdb"
    
    if not os.path.exists(db_file):
        print(f"❌ 数据库文件不存在: {db_file}")
        return
    
    print(f"\n🔍 开始监控数据更新 ({duration_minutes} 分钟)...")
    
    start_time = time.time()
    last_count = 0
    
    try:
        while time.time() - start_time < duration_minutes * 60:
            conn = duckdb.connect(db_file, read_only=True)
            current_count = conn.execute("SELECT COUNT(*) FROM ts_data").fetchone()[0]
            
            if current_count != last_count:
                change = current_count - last_count
                timestamp = datetime.now().strftime('%H:%M:%S')
                print(f"[{timestamp}] 📊 记录数变化: {last_count:,} → {current_count:,} ({change:+,})")
                last_count = current_count
            
            conn.close()
            time.sleep(10)  # 每10秒检查一次
            
    except KeyboardInterrupt:
        print("\n⏹️  监控已停止")
    except Exception as e:
        print(f"❌ 监控过程中出错: {e}")

def main():
    """主函数"""
    print("🚀 实时数据缓存服务测试工具")
    print("=" * 50)
    
    # 基本连接测试
    if not test_duckdb_connection():
        return
    
    # 询问是否进行监控测试
    try:
        response = input("\n是否要监控数据更新？(y/N): ").strip().lower()
        if response in ['y', 'yes']:
            monitor_data_updates()
    except KeyboardInterrupt:
        print("\n👋 测试结束")

if __name__ == "__main__":
    main()