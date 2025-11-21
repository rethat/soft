"""
Report Generator for Load Test Results
Creates comprehensive reports with charts comparing Couchbase and MongoDB performance
"""
import os
import sys
import json
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from datetime import datetime
from typing import Dict, List
import numpy as np

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from logger_config import get_logger

logger = get_logger(__name__)

# Set style for better-looking charts
try:
    plt.style.use('seaborn-v0_8-darkgrid')
except:
    try:
        plt.style.use('seaborn-darkgrid')
    except:
        plt.style.use('default')
matplotlib.rcParams['figure.figsize'] = (12, 8)
matplotlib.rcParams['font.size'] = 10


class ReportGenerator:
    """Generates performance comparison reports with charts"""
    
    def __init__(self, output_dir: str = "reports"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(os.path.join(output_dir, "charts"), exist_ok=True)
        
    def generate_report(self, test_results: List[Dict], bucket_name: str = "test", 
                       collection_name: str = "test") -> str:
        """
        Generate comprehensive report from test results
        
        Args:
            test_results: List of statistics dictionaries from load tests
            bucket_name: Name of the bucket/collection tested
            collection_name: Name of the collection (for MongoDB)
        
        Returns:
            Path to generated report file
        """
        logger.info(f"Generating report for {bucket_name}/{collection_name}")
        
        # Generate charts
        self._generate_response_time_chart(test_results, bucket_name)
        self._generate_throughput_chart(test_results, bucket_name)
        self._generate_success_rate_chart(test_results, bucket_name)
        self._generate_percentile_chart(test_results, bucket_name)
        self._generate_comparison_chart(test_results, bucket_name)
        self._generate_response_time_distribution(test_results, bucket_name)
        
        # Generate HTML report
        report_path = self._generate_html_report(test_results, bucket_name, collection_name)
        
        logger.info(f"Report generated: {report_path}")
        return report_path
    
    def _generate_response_time_chart(self, test_results: List[Dict], bucket_name: str):
        """Generate response time comparison chart"""
        fig, ax = plt.subplots(figsize=(14, 8))
        
        users = [r['num_users'] for r in test_results]
        cb_avg = [r['couchbase']['avg_response_time'] * 1000 for r in test_results]  # Convert to ms
        mongo_avg = [r['mongodb']['avg_response_time'] * 1000 for r in test_results]
        
        x = np.arange(len(users))
        width = 0.35
        
        bars1 = ax.bar(x - width/2, cb_avg, width, label='Couchbase', color='#1f77b4', alpha=0.8)
        bars2 = ax.bar(x + width/2, mongo_avg, width, label='MongoDB', color='#ff7f0e', alpha=0.8)
        
        ax.set_xlabel('Số lượng Concurrent Users', fontsize=12, fontweight='bold')
        ax.set_ylabel('Thời gian phản hồi trung bình (ms)', fontsize=12, fontweight='bold')
        ax.set_title(f'So sánh thời gian phản hồi trung bình - {bucket_name}', 
                    fontsize=14, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels([f'{u} users' for u in users])
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3, axis='y')
        
        # Add value labels on bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{height:.1f}ms',
                       ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        chart_path = os.path.join(self.output_dir, "charts", f"{bucket_name}_response_time.png")
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
    
    def _generate_throughput_chart(self, test_results: List[Dict], bucket_name: str):
        """Generate throughput comparison chart"""
        fig, ax = plt.subplots(figsize=(14, 8))
        
        users = [r['num_users'] for r in test_results]
        cb_throughput = [r['couchbase']['throughput_qps'] for r in test_results]
        mongo_throughput = [r['mongodb']['throughput_qps'] for r in test_results]
        
        x = np.arange(len(users))
        width = 0.35
        
        bars1 = ax.bar(x - width/2, cb_throughput, width, label='Couchbase', color='#1f77b4', alpha=0.8)
        bars2 = ax.bar(x + width/2, mongo_throughput, width, label='MongoDB', color='#ff7f0e', alpha=0.8)
        
        ax.set_xlabel('Số lượng Concurrent Users', fontsize=12, fontweight='bold')
        ax.set_ylabel('Throughput (Queries/Second)', fontsize=12, fontweight='bold')
        ax.set_title(f'So sánh Throughput - {bucket_name}', 
                    fontsize=14, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels([f'{u} users' for u in users])
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3, axis='y')
        
        # Add value labels on bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{height:.1f}',
                       ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        chart_path = os.path.join(self.output_dir, "charts", f"{bucket_name}_throughput.png")
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
    
    def _generate_success_rate_chart(self, test_results: List[Dict], bucket_name: str):
        """Generate success rate comparison chart"""
        fig, ax = plt.subplots(figsize=(14, 8))
        
        users = [r['num_users'] for r in test_results]
        cb_success = [r['couchbase']['success_rate'] for r in test_results]
        mongo_success = [r['mongodb']['success_rate'] for r in test_results]
        
        x = np.arange(len(users))
        width = 0.35
        
        bars1 = ax.bar(x - width/2, cb_success, width, label='Couchbase', color='#2ca02c', alpha=0.8)
        bars2 = ax.bar(x + width/2, mongo_success, width, label='MongoDB', color='#d62728', alpha=0.8)
        
        ax.set_xlabel('Số lượng Concurrent Users', fontsize=12, fontweight='bold')
        ax.set_ylabel('Tỷ lệ thành công (%)', fontsize=12, fontweight='bold')
        ax.set_title(f'So sánh tỷ lệ thành công - {bucket_name}', 
                    fontsize=14, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels([f'{u} users' for u in users])
        ax.set_ylim([0, 105])
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3, axis='y')
        
        # Add value labels on bars
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{height:.1f}%',
                       ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        chart_path = os.path.join(self.output_dir, "charts", f"{bucket_name}_success_rate.png")
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
    
    def _generate_percentile_chart(self, test_results: List[Dict], bucket_name: str):
        """Generate percentile comparison chart (P50, P95, P99)"""
        fig, ax = plt.subplots(figsize=(14, 8))
        
        users = [r['num_users'] for r in test_results]
        
        # Get percentiles for each database
        cb_p50 = [r['couchbase']['median_response_time'] * 1000 for r in test_results]
        cb_p95 = [r['couchbase']['p95_response_time'] * 1000 for r in test_results]
        cb_p99 = [r['couchbase']['p99_response_time'] * 1000 for r in test_results]
        
        mongo_p50 = [r['mongodb']['median_response_time'] * 1000 for r in test_results]
        mongo_p95 = [r['mongodb']['p95_response_time'] * 1000 for r in test_results]
        mongo_p99 = [r['mongodb']['p99_response_time'] * 1000 for r in test_results]
        
        x = np.arange(len(users))
        width = 0.12
        
        # Couchbase bars
        ax.bar(x - width*2.5, cb_p50, width, label='Couchbase P50', color='#1f77b4', alpha=0.8)
        ax.bar(x - width*1.5, cb_p95, width, label='Couchbase P95', color='#1f77b4', alpha=0.6)
        ax.bar(x - width*0.5, cb_p99, width, label='Couchbase P99', color='#1f77b4', alpha=0.4)
        
        # MongoDB bars
        ax.bar(x + width*0.5, mongo_p50, width, label='MongoDB P50', color='#ff7f0e', alpha=0.8)
        ax.bar(x + width*1.5, mongo_p95, width, label='MongoDB P95', color='#ff7f0e', alpha=0.6)
        ax.bar(x + width*2.5, mongo_p99, width, label='MongoDB P99', color='#ff7f0e', alpha=0.4)
        
        ax.set_xlabel('Số lượng Concurrent Users', fontsize=12, fontweight='bold')
        ax.set_ylabel('Thời gian phản hồi (ms)', fontsize=12, fontweight='bold')
        ax.set_title(f'So sánh Percentiles (P50, P95, P99) - {bucket_name}', 
                    fontsize=14, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels([f'{u} users' for u in users])
        ax.legend(fontsize=10, ncol=2)
        ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        chart_path = os.path.join(self.output_dir, "charts", f"{bucket_name}_percentiles.png")
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
    
    def _generate_comparison_chart(self, test_results: List[Dict], bucket_name: str):
        """Generate comprehensive comparison chart with multiple metrics"""
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle(f'Tổng quan so sánh hiệu năng - {bucket_name}', 
                    fontsize=16, fontweight='bold', y=0.995)
        
        users = [r['num_users'] for r in test_results]
        x = np.arange(len(users))
        width = 0.35
        
        # 1. Response Time
        ax = axes[0, 0]
        cb_avg = [r['couchbase']['avg_response_time'] * 1000 for r in test_results]
        mongo_avg = [r['mongodb']['avg_response_time'] * 1000 for r in test_results]
        ax.bar(x - width/2, cb_avg, width, label='Couchbase', color='#1f77b4', alpha=0.8)
        ax.bar(x + width/2, mongo_avg, width, label='MongoDB', color='#ff7f0e', alpha=0.8)
        ax.set_xlabel('Concurrent Users')
        ax.set_ylabel('Response Time (ms)')
        ax.set_title('Average Response Time')
        ax.set_xticks(x)
        ax.set_xticklabels([f'{u}' for u in users])
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        
        # 2. Throughput
        ax = axes[0, 1]
        cb_throughput = [r['couchbase']['throughput_qps'] for r in test_results]
        mongo_throughput = [r['mongodb']['throughput_qps'] for r in test_results]
        ax.bar(x - width/2, cb_throughput, width, label='Couchbase', color='#1f77b4', alpha=0.8)
        ax.bar(x + width/2, mongo_throughput, width, label='MongoDB', color='#ff7f0e', alpha=0.8)
        ax.set_xlabel('Concurrent Users')
        ax.set_ylabel('Queries/Second')
        ax.set_title('Throughput')
        ax.set_xticks(x)
        ax.set_xticklabels([f'{u}' for u in users])
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        
        # 3. Success Rate
        ax = axes[1, 0]
        cb_success = [r['couchbase']['success_rate'] for r in test_results]
        mongo_success = [r['mongodb']['success_rate'] for r in test_results]
        ax.bar(x - width/2, cb_success, width, label='Couchbase', color='#2ca02c', alpha=0.8)
        ax.bar(x + width/2, mongo_success, width, label='MongoDB', color='#d62728', alpha=0.8)
        ax.set_xlabel('Concurrent Users')
        ax.set_ylabel('Success Rate (%)')
        ax.set_title('Success Rate')
        ax.set_xticks(x)
        ax.set_xticklabels([f'{u}' for u in users])
        ax.set_ylim([0, 105])
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        
        # 4. P95 Response Time
        ax = axes[1, 1]
        cb_p95 = [r['couchbase']['p95_response_time'] * 1000 for r in test_results]
        mongo_p95 = [r['mongodb']['p95_response_time'] * 1000 for r in test_results]
        ax.bar(x - width/2, cb_p95, width, label='Couchbase', color='#1f77b4', alpha=0.8)
        ax.bar(x + width/2, mongo_p95, width, label='MongoDB', color='#ff7f0e', alpha=0.8)
        ax.set_xlabel('Concurrent Users')
        ax.set_ylabel('Response Time (ms)')
        ax.set_title('P95 Response Time')
        ax.set_xticks(x)
        ax.set_xticklabels([f'{u}' for u in users])
        ax.legend()
        ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        chart_path = os.path.join(self.output_dir, "charts", f"{bucket_name}_comparison.png")
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
    
    def _generate_response_time_distribution(self, test_results: List[Dict], bucket_name: str):
        """Generate response time distribution chart"""
        # This would require raw response time data, which we'll get from saved results
        # For now, we'll create a chart showing min/max/avg ranges
        fig, ax = plt.subplots(figsize=(14, 8))
        
        users = [r['num_users'] for r in test_results]
        
        cb_min = [r['couchbase']['min_response_time'] * 1000 for r in test_results]
        cb_max = [r['couchbase']['max_response_time'] * 1000 for r in test_results]
        cb_avg = [r['couchbase']['avg_response_time'] * 1000 for r in test_results]
        
        mongo_min = [r['mongodb']['min_response_time'] * 1000 for r in test_results]
        mongo_max = [r['mongodb']['max_response_time'] * 1000 for r in test_results]
        mongo_avg = [r['mongodb']['avg_response_time'] * 1000 for r in test_results]
        
        x = np.arange(len(users))
        width = 0.35
        
        # Plot ranges as error bars
        ax.errorbar(x - width/2, cb_avg, yerr=[np.array(cb_avg) - np.array(cb_min), 
                                                np.array(cb_max) - np.array(cb_avg)],
                   fmt='o', label='Couchbase', color='#1f77b4', capsize=5, capthick=2, linewidth=2)
        ax.errorbar(x + width/2, mongo_avg, yerr=[np.array(mongo_avg) - np.array(mongo_min),
                                                   np.array(mongo_max) - np.array(mongo_avg)],
                   fmt='o', label='MongoDB', color='#ff7f0e', capsize=5, capthick=2, linewidth=2)
        
        ax.set_xlabel('Số lượng Concurrent Users', fontsize=12, fontweight='bold')
        ax.set_ylabel('Thời gian phản hồi (ms)', fontsize=12, fontweight='bold')
        ax.set_title(f'Phân bố thời gian phản hồi (Min/Avg/Max) - {bucket_name}', 
                    fontsize=14, fontweight='bold', pad=20)
        ax.set_xticks(x)
        ax.set_xticklabels([f'{u} users' for u in users])
        ax.legend(fontsize=11)
        ax.grid(True, alpha=0.3, axis='y')
        
        plt.tight_layout()
        chart_path = os.path.join(self.output_dir, "charts", f"{bucket_name}_distribution.png")
        plt.savefig(chart_path, dpi=300, bbox_inches='tight')
        plt.close()
    
    def _generate_html_report(self, test_results: List[Dict], bucket_name: str, 
                             collection_name: str) -> str:
        """Generate HTML report with all charts and statistics"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        html = f"""
<!DOCTYPE html>
<html lang="vi">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Báo cáo Load Test - {bucket_name}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            border-bottom: 3px solid #1f77b4;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #555;
            margin-top: 30px;
            border-left: 4px solid #1f77b4;
            padding-left: 15px;
        }}
        .summary {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .summary-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        .summary-card h3 {{
            margin: 0 0 10px 0;
            font-size: 14px;
            opacity: 0.9;
        }}
        .summary-card .value {{
            font-size: 32px;
            font-weight: bold;
            margin: 10px 0;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        th {{
            background-color: #1f77b4;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: bold;
        }}
        td {{
            padding: 10px 12px;
            border-bottom: 1px solid #ddd;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        tr:hover {{
            background-color: #f0f0f0;
        }}
        .chart-container {{
            margin: 30px 0;
            text-align: center;
        }}
        .chart-container img {{
            max-width: 100%;
            height: auto;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        .comparison-table {{
            margin: 20px 0;
        }}
        .better {{
            color: #2ca02c;
            font-weight: bold;
        }}
        .worse {{
            color: #d62728;
            font-weight: bold;
        }}
        .timestamp {{
            color: #888;
            font-size: 14px;
            text-align: right;
            margin-top: 20px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Báo cáo Load Test và So sánh Hiệu năng</h1>
        <p class="timestamp">Thời gian tạo: {timestamp}</p>
        
        <h2>Thông tin Test</h2>
        <ul>
            <li><strong>Bucket/Collection:</strong> {bucket_name} / {collection_name}</li>
            <li><strong>Số lượng test scenarios:</strong> {len(test_results)}</li>
        </ul>
        
        <h2>Tóm tắt Kết quả</h2>
        <div class="summary">
"""
        
        # Calculate overall statistics
        all_cb_avg = [r['couchbase']['avg_response_time'] * 1000 for r in test_results]
        all_mongo_avg = [r['mongodb']['avg_response_time'] * 1000 for r in test_results]
        all_cb_throughput = [r['couchbase']['throughput_qps'] for r in test_results]
        all_mongo_throughput = [r['mongodb']['throughput_qps'] for r in test_results]
        
        html += f"""
            <div class="summary-card">
                <h3>Couchbase - Response Time Trung bình</h3>
                <div class="value">{sum(all_cb_avg)/len(all_cb_avg):.2f} ms</div>
            </div>
            <div class="summary-card">
                <h3>MongoDB - Response Time Trung bình</h3>
                <div class="value">{sum(all_mongo_avg)/len(all_mongo_avg):.2f} ms</div>
            </div>
            <div class="summary-card">
                <h3>Couchbase - Throughput Trung bình</h3>
                <div class="value">{sum(all_cb_throughput)/len(all_cb_throughput):.2f} QPS</div>
            </div>
            <div class="summary-card">
                <h3>MongoDB - Throughput Trung bình</h3>
                <div class="value">{sum(all_mongo_throughput)/len(all_mongo_throughput):.2f} QPS</div>
            </div>
        </div>
        
        <h2>Biểu đồ So sánh</h2>
        
        <div class="chart-container">
            <h3>1. So sánh Thời gian Phản hồi Trung bình</h3>
            <img src="charts/{bucket_name}_response_time.png" alt="Response Time Comparison">
        </div>
        
        <div class="chart-container">
            <h3>2. So sánh Throughput</h3>
            <img src="charts/{bucket_name}_throughput.png" alt="Throughput Comparison">
        </div>
        
        <div class="chart-container">
            <h3>3. So sánh Tỷ lệ Thành công</h3>
            <img src="charts/{bucket_name}_success_rate.png" alt="Success Rate Comparison">
        </div>
        
        <div class="chart-container">
            <h3>4. So sánh Percentiles (P50, P95, P99)</h3>
            <img src="charts/{bucket_name}_percentiles.png" alt="Percentiles Comparison">
        </div>
        
        <div class="chart-container">
            <h3>5. Tổng quan So sánh</h3>
            <img src="charts/{bucket_name}_comparison.png" alt="Overall Comparison">
        </div>
        
        <div class="chart-container">
            <h3>6. Phân bố Thời gian Phản hồi</h3>
            <img src="charts/{bucket_name}_distribution.png" alt="Response Time Distribution">
        </div>
        
        <h2>Chi tiết Kết quả theo Số lượng Users</h2>
"""
        
        # Generate detailed tables for each test scenario
        for i, result in enumerate(test_results):
            num_users = result['num_users']
            cb_stats = result['couchbase']
            mongo_stats = result['mongodb']
            
            html += f"""
        <h3>Test với {num_users} Concurrent Users</h3>
        <table class="comparison-table">
            <thead>
                <tr>
                    <th>Metric</th>
                    <th>Couchbase</th>
                    <th>MongoDB</th>
                    <th>Kết luận</th>
                </tr>
            </thead>
            <tbody>
                <tr>
                    <td><strong>Response Time Trung bình (ms)</strong></td>
                    <td>{cb_stats['avg_response_time'] * 1000:.2f}</td>
                    <td>{mongo_stats['avg_response_time'] * 1000:.2f}</td>
                    <td>{'<span class="better">Couchbase tốt hơn</span>' if cb_stats['avg_response_time'] < mongo_stats['avg_response_time'] else '<span class="worse">MongoDB tốt hơn</span>'}</td>
                </tr>
                <tr>
                    <td><strong>Response Time Min (ms)</strong></td>
                    <td>{cb_stats['min_response_time'] * 1000:.2f}</td>
                    <td>{mongo_stats['min_response_time'] * 1000:.2f}</td>
                    <td>{'<span class="better">Couchbase tốt hơn</span>' if cb_stats['min_response_time'] < mongo_stats['min_response_time'] else '<span class="worse">MongoDB tốt hơn</span>'}</td>
                </tr>
                <tr>
                    <td><strong>Response Time Max (ms)</strong></td>
                    <td>{cb_stats['max_response_time'] * 1000:.2f}</td>
                    <td>{mongo_stats['max_response_time'] * 1000:.2f}</td>
                    <td>{'<span class="better">Couchbase tốt hơn</span>' if cb_stats['max_response_time'] < mongo_stats['max_response_time'] else '<span class="worse">MongoDB tốt hơn</span>'}</td>
                </tr>
                <tr>
                    <td><strong>Response Time P95 (ms)</strong></td>
                    <td>{cb_stats['p95_response_time'] * 1000:.2f}</td>
                    <td>{mongo_stats['p95_response_time'] * 1000:.2f}</td>
                    <td>{'<span class="better">Couchbase tốt hơn</span>' if cb_stats['p95_response_time'] < mongo_stats['p95_response_time'] else '<span class="worse">MongoDB tốt hơn</span>'}</td>
                </tr>
                <tr>
                    <td><strong>Response Time P99 (ms)</strong></td>
                    <td>{cb_stats['p99_response_time'] * 1000:.2f}</td>
                    <td>{mongo_stats['p99_response_time'] * 1000:.2f}</td>
                    <td>{'<span class="better">Couchbase tốt hơn</span>' if cb_stats['p99_response_time'] < mongo_stats['p99_response_time'] else '<span class="worse">MongoDB tốt hơn</span>'}</td>
                </tr>
                <tr>
                    <td><strong>Throughput (QPS)</strong></td>
                    <td>{cb_stats['throughput_qps']:.2f}</td>
                    <td>{mongo_stats['throughput_qps']:.2f}</td>
                    <td>{'<span class="better">Couchbase tốt hơn</span>' if cb_stats['throughput_qps'] > mongo_stats['throughput_qps'] else '<span class="worse">MongoDB tốt hơn</span>'}</td>
                </tr>
                <tr>
                    <td><strong>Tỷ lệ Thành công (%)</strong></td>
                    <td>{cb_stats['success_rate']:.2f}%</td>
                    <td>{mongo_stats['success_rate']:.2f}%</td>
                    <td>{'<span class="better">Couchbase tốt hơn</span>' if cb_stats['success_rate'] > mongo_stats['success_rate'] else '<span class="worse">MongoDB tốt hơn</span>'}</td>
                </tr>
                <tr>
                    <td><strong>Tổng số Queries</strong></td>
                    <td>{cb_stats['total_queries']}</td>
                    <td>{mongo_stats['total_queries']}</td>
                    <td>-</td>
                </tr>
                <tr>
                    <td><strong>Số Queries Thành công</strong></td>
                    <td>{cb_stats['successful_queries']}</td>
                    <td>{mongo_stats['successful_queries']}</td>
                    <td>-</td>
                </tr>
                <tr>
                    <td><strong>Số Queries Thất bại</strong></td>
                    <td>{cb_stats['failed_queries']}</td>
                    <td>{mongo_stats['failed_queries']}</td>
                    <td>-</td>
                </tr>
            </tbody>
        </table>
"""
        
        html += """
        <h2>Kết luận</h2>
        <p>Báo cáo này so sánh hiệu năng giữa Couchbase và MongoDB dưới các điều kiện tải khác nhau. 
        Các metric chính được đánh giá bao gồm:</p>
        <ul>
            <li><strong>Response Time:</strong> Thời gian phản hồi của mỗi query</li>
            <li><strong>Throughput:</strong> Số lượng queries có thể xử lý mỗi giây</li>
            <li><strong>Success Rate:</strong> Tỷ lệ queries thành công</li>
            <li><strong>Percentiles:</strong> P50 (median), P95, P99 để đánh giá độ ổn định</li>
        </ul>
        <p><strong>Lưu ý:</strong> Kết quả có thể thay đổi tùy thuộc vào cấu hình hệ thống, 
        kích thước dữ liệu, và điều kiện mạng tại thời điểm test.</p>
    </div>
</body>
</html>
"""
        
        report_path = os.path.join(self.output_dir, f"{bucket_name}_load_test_report.html")
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        return report_path

