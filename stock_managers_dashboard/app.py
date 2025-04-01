import streamlit as st
import pymongo
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import numpy as np

# Page configuration
st.set_page_config(
    page_title="上市公司管理层数据分析",
    page_icon="📊",
    layout="wide"
)

# MongoDB connection function
@st.cache_resource
def get_mongodb_client():
    # Connect to MongoDB
    client = pymongo.MongoClient(
        "mongodb://adminUser:MongoAdmin%402025%21@106.14.185.239:27017/?authSource=admin&authMechanism=SCRAM-SHA-1"
    )
    return client

# Load data function
@st.cache_data(ttl=3600)
def load_data(start_date=None, end_date=None, ts_codes=None):
    client = get_mongodb_client()
    db = client['tushare_data']
    collection = db['stk_managers']
    
    # Build query
    query = {}
    if start_date and end_date:
        query['ann_date'] = {'$gte': start_date, '$lte': end_date}
    if ts_codes and len(ts_codes) > 0:
        query['ts_code'] = {'$in': ts_codes}
    
    # Execute query
    data = list(collection.find(query))
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    # Clean data if not empty
    if not df.empty:
        # Replace None with NaN
        df = df.replace({None: np.nan})
        
    return df

# Get unique stock codes
@st.cache_data(ttl=3600)
def get_stock_codes():
    client = get_mongodb_client()
    db = client['tushare_data']
    collection = db['stk_managers']
    
    # Get unique stock codes
    stock_codes = collection.distinct('ts_code')
    return sorted(stock_codes)

# Get stock names from stock_basic collection
@st.cache_data(ttl=3600)
def get_stock_names():
    client = get_mongodb_client()
    db = client['tushare_data']
    collection = db['stock_basic']
    
    # Get stock names
    stock_data = list(collection.find({}, {'ts_code': 1, 'name': 1}))
    stock_df = pd.DataFrame(stock_data)
    
    if not stock_df.empty:
        return dict(zip(stock_df['ts_code'], stock_df['name']))
    return {}

# Function to create a hierarchical org chart
def create_org_chart(df, ts_code):
    if df.empty:
        return None
        
    # Filter by selected stock code
    df_filtered = df[df['ts_code'] == ts_code].copy()
    
    if df_filtered.empty:
        return None
        
    # Sort by level and title
    level_order = ['高管', '董事会成员', '监事']
    df_filtered['lev_order'] = pd.Categorical(
        df_filtered['lev'], 
        categories=level_order, 
        ordered=True
    )
    
    df_filtered = df_filtered.sort_values(['lev_order', 'title'])
    
    # Create nodes and links for the org chart
    nodes = []
    links = []
    
    # Add company as root node
    nodes.append({
        'id': 'company',
        'label': ts_code,
        'level': 0,
        'shape': 'circle',
        'color': '#1f77b4'
    })
    
    # Add level nodes (高管, 董事会成员, 监事)
    for i, level in enumerate(level_order):
        nodes.append({
            'id': f'level_{i}',
            'label': level,
            'level': 1,
            'shape': 'circle',
            'color': '#ff7f0e'
        })
        links.append({
            'source': 'company',
            'target': f'level_{i}'
        })
    
    # Add person nodes
    for i, row in df_filtered.iterrows():
        level_idx = level_order.index(row['lev']) if row['lev'] in level_order else 0
        person_id = f"person_{i}"
        
        edu = row['edu'] if pd.notna(row['edu']) else '未知'
        gender = '男' if row['gender'] == 'M' else '女' if row['gender'] == 'F' else '未知'
        
        nodes.append({
            'id': person_id,
            'label': f"{row['name']} ({row['title']})",
            'level': 2,
            'shape': 'circle',
            'color': '#2ca02c',
            'details': {
                'name': row['name'],
                'title': row['title'],
                'gender': gender,
                'education': edu,
                'birthday': row['birthday'] if pd.notna(row['birthday']) else '未知',
                'start_date': row['begin_date'] if pd.notna(row['begin_date']) else '未知'
            }
        })
        
        links.append({
            'source': f'level_{level_idx}',
            'target': person_id
        })
    
    return {'nodes': nodes, 'links': links}

# Main application
def main():
    st.title("上市公司管理层数据分析")
    
    # Get stock codes and names
    stock_codes = get_stock_codes()
    stock_names = get_stock_names()
    
    # Create stock code options with names if available
    stock_options = []
    for code in stock_codes:
        if code in stock_names:
            stock_options.append(f"{code} - {stock_names[code]}")
        else:
            stock_options.append(code)
    
    # Sidebar filters
    st.sidebar.header("过滤条件")
    
    # Date range selection
    today = datetime.now()
    default_start = (today - timedelta(days=365)).strftime('%Y%m%d')
    default_end = today.strftime('%Y%m%d')
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input(
            "开始日期",
            datetime.strptime(default_start, '%Y%m%d'),
            format="YYYY-MM-DD"
        )
    with col2:
        end_date = st.date_input(
            "结束日期",
            datetime.strptime(default_end, '%Y%m%d'),
            format="YYYY-MM-DD"
        )
    
    # Convert dates to string format expected by MongoDB
    start_date_str = start_date.strftime('%Y%m%d')
    end_date_str = end_date.strftime('%Y%m%d')
    
    # Stock code selection
    selected_stocks = st.sidebar.multiselect(
        "选择股票代码",
        options=stock_options
    )
    
    # Extract actual stock codes from selected options
    selected_ts_codes = []
    for stock in selected_stocks:
        code = stock.split(" - ")[0]
        selected_ts_codes.append(code)
    
    # Position type selection
    position_types = ["高管", "董事会成员", "监事"]
    selected_positions = st.sidebar.multiselect(
        "选择职位类型",
        options=position_types,
        default=position_types
    )
    
    # Load data
    df = load_data(start_date_str, end_date_str, selected_ts_codes)
    
    # Apply position filter
    if selected_positions and not df.empty and 'lev' in df.columns:
        df = df[df['lev'].isin(selected_positions)]
    
    # Display data statistics
    st.header("数据概览")
    
    if df.empty:
        st.warning("没有找到符合条件的数据。请调整过滤条件后重试。")
        return
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("公司数量", df['ts_code'].nunique())
    with col2:
        st.metric("管理层人员数量", len(df))
    with col3:
        st.metric("数据时间范围", f"{df['ann_date'].min()} - {df['ann_date'].max()}")
    
    # Create tabs for different visualizations
    tab1, tab2, tab3, tab4, tab5 = st.tabs(["性别分布", "教育水平", "管理层结构", "年龄分布", "原始数据"])
    
    with tab1:
        st.header("管理层性别分布")
        
        if 'gender' in df.columns:
            # Replace codes with readable labels
            gender_map = {'M': '男', 'F': '女'}
            df_gender = df.copy()
            df_gender['gender'] = df_gender['gender'].map(gender_map).fillna('未知')
            
            # Count by gender
            gender_counts = df_gender['gender'].value_counts().reset_index()
            gender_counts.columns = ['性别', '人数']
            
            # Create pie chart
            fig = px.pie(
                gender_counts, 
                values='人数', 
                names='性别', 
                title='管理层性别比例',
                color='性别',
                color_discrete_map={'男': '#1f77b4', '女': '#ff7f0e', '未知': '#7f7f7f'}
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
            
            # Gender distribution by position level
            if 'lev' in df.columns:
                st.subheader("不同职位的性别分布")
                gender_by_level = pd.crosstab(df_gender['lev'], df_gender['gender'])
                
                # Convert to percentage
                gender_by_level_pct = gender_by_level.div(gender_by_level.sum(axis=1), axis=0) * 100
                
                # Create stacked bar chart
                fig = px.bar(
                    gender_by_level_pct, 
                    title="各职位层级性别比例",
                    labels={'value': '比例 (%)', 'gender': '性别', 'lev': '职位层级'},
                    color_discrete_map={'男': '#1f77b4', '女': '#ff7f0e', '未知': '#7f7f7f'}
                )
                fig.update_layout(legend_title_text='性别')
                st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.header("管理层教育水平分布")
        
        if 'edu' in df.columns:
            # Count by education
            edu_counts = df['edu'].fillna('未知').value_counts().reset_index()
            edu_counts.columns = ['教育水平', '人数']
            
            # Order by education level
            edu_order = ['博士', '硕士', 'MBA', '本科', '大专', '高中', '初中', '其他', '未知']
            edu_counts['教育水平'] = pd.Categorical(
                edu_counts['教育水平'], 
                categories=edu_order, 
                ordered=True
            )
            edu_counts = edu_counts.sort_values('教育水平')
            
            # Create bar chart
            fig = px.bar(
                edu_counts, 
                x='教育水平', 
                y='人数',
                title='管理层教育水平分布',
                color='教育水平'
            )
            fig.update_layout(xaxis_title='教育水平', yaxis_title='人数')
            st.plotly_chart(fig, use_container_width=True)
            
            # Education by position level
            if 'lev' in df.columns:
                st.subheader("不同职位的教育水平分布")
                df_edu = df.copy()
                df_edu['edu'] = df_edu['edu'].fillna('未知')
                
                # Simplify education categories
                edu_simplified = {
                    '博士': '博士',
                    '硕士': '硕士',
                    'MBA': '硕士',
                    '本科': '本科',
                    '大专': '专科及以下',
                    '高中': '专科及以下',
                    '初中': '专科及以下',
                    '其他': '其他',
                    '未知': '未知'
                }
                
                df_edu['edu_simple'] = df_edu['edu'].map(
                    lambda x: edu_simplified.get(x, '其他')
                )
                
                # Create crosstab
                edu_by_level = pd.crosstab(df_edu['lev'], df_edu['edu_simple'])
                
                # Convert to percentage
                edu_by_level_pct = edu_by_level.div(edu_by_level.sum(axis=1), axis=0) * 100
                
                # Create stacked bar chart
                fig = px.bar(
                    edu_by_level_pct, 
                    title="各职位层级教育水平比例",
                    labels={'value': '比例 (%)', 'edu_simple': '教育水平', 'lev': '职位层级'},
                    barmode='stack'
                )
                fig.update_layout(legend_title_text='教育水平')
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.header("管理层结构分析")
        
        if 'lev' in df.columns and 'title' in df.columns:
            # Position level distribution
            level_counts = df['lev'].value_counts().reset_index()
            level_counts.columns = ['职位层级', '人数']
            
            fig = px.pie(
                level_counts, 
                values='人数', 
                names='职位层级', 
                title='管理层职位层级分布'
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
            
            # Title distribution
            title_counts = df['title'].value_counts().head(15).reset_index()
            title_counts.columns = ['职位', '人数']
            
            fig = px.bar(
                title_counts, 
                x='人数', 
                y='职位',
                title='管理层常见职位分布（Top 15）',
                orientation='h'
            )
            fig.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
            
            # Company organization chart (for selected company)
            if len(selected_ts_codes) == 1:
                st.subheader("公司组织结构图")
                
                org_data = create_org_chart(df, selected_ts_codes[0])
                
                if org_data:
                    # Create network graph
                    import networkx as nx
                    import matplotlib.pyplot as plt
                    from matplotlib.patches import Circle
                    
                    G = nx.DiGraph()
                    
                    # Add nodes
                    for node in org_data['nodes']:
                        G.add_node(
                            node['id'], 
                            label=node['label'], 
                            level=node['level'],
                            color=node['color'],
                            details=node.get('details', {})
                        )
                    
                    # Add edges
                    for link in org_data['links']:
                        G.add_edge(link['source'], link['target'])
                    
                    # Create positions using hierarchical layout
                    pos = nx.multipartite_layout(G, subset_key='level')
                    
                    # Create figure
                    fig, ax = plt.subplots(figsize=(12, 8))
                    
                    # Draw nodes with different colors by level
                    node_colors = [G.nodes[n]['color'] for n in G.nodes]
                    nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=700, alpha=0.8)
                    
                    # Draw edges
                    nx.draw_networkx_edges(G, pos, edge_color='gray', arrows=True)
                    
                    # Draw labels
                    nx.draw_networkx_labels(G, pos, font_size=10, font_weight='bold')
                    
                    plt.title(f"{selected_ts_codes[0]} 组织结构图")
                    plt.axis('off')
                    st.pyplot(fig)
                    
                    # Show details table
                    st.subheader(f"{selected_ts_codes[0]} 管理层详情")
                    detail_cols = ['name', 'title', 'gender', 'edu', 'birthday', 'begin_date', 'end_date', 'lev', 'national']
                    
                    # Rename columns for display
                    col_rename = {
                        'name': '姓名',
                        'title': '职位',
                        'gender': '性别',
                        'edu': '教育水平',
                        'birthday': '出生年份',
                        'begin_date': '任职开始',
                        'end_date': '任职结束',
                        'lev': '层级',
                        'national': '国籍'
                    }
                    
                    # Replace gender codes
                    df_detail = df[df['ts_code'] == selected_ts_codes[0]].copy()
                    df_detail['gender'] = df_detail['gender'].map({'M': '男', 'F': '女'}).fillna('未知')
                    
                    # Select and rename columns
                    df_display = df_detail[detail_cols].rename(columns=col_rename)
                    
                    st.dataframe(
                        df_display,
                        use_container_width=True,
                        hide_index=True
                    )
                else:
                    st.warning(f"没有足够的数据创建 {selected_ts_codes[0]} 的组织结构图")
            else:
                st.info("请选择单个股票代码以查看组织结构图")
    
    with tab4:
        st.header("管理层年龄分布")
        
        if 'birthday' in df.columns and not df['birthday'].isna().all():
            # Calculate age based on birth year
            current_year = datetime.now().year
            df_age = df.copy()
            
            # Convert birthday to birth year
            df_age['birth_year'] = pd.to_numeric(df_age['birthday'].str[:4], errors='coerce')
            
            # Calculate age
            df_age['age'] = current_year - df_age['birth_year']
            
            # Filter out invalid ages
            df_age = df_age[(df_age['age'] >= 0) & (df_age['age'] < 100)]
            
            if not df_age.empty:
                # Create histogram
                fig = px.histogram(
                    df_age, 
                    x='age',
                    title='管理层年龄分布',
                    labels={'age': '年龄', 'count': '人数'},
                    nbins=20
                )
                fig.update_layout(bargap=0.1)
                st.plotly_chart(fig, use_container_width=True)
                
                # Age statistics
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("平均年龄", f"{df_age['age'].mean():.1f}岁")
                with col2:
                    st.metric("中位年龄", f"{df_age['age'].median():.1f}岁")
                with col3:
                    st.metric("最小年龄", f"{df_age['age'].min()}岁")
                with col4:
                    st.metric("最大年龄", f"{df_age['age'].max()}岁")
                
                # Age by position level
                if 'lev' in df.columns:
                    st.subheader("不同职位层级的年龄分布")
                    
                    fig = px.box(
                        df_age,
                        x='lev',
                        y='age',
                        title='各职位层级年龄分布',
                        labels={'lev': '职位层级', 'age': '年龄'},
                        points='all'
                    )
                    fig.update_layout(xaxis_title='职位层级', yaxis_title='年龄')
                    st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("无有效的年龄数据可供分析")
        else:
            st.warning("没有年龄相关数据可供分析")
    
    with tab5:
        st.header("原始数据")
        
        # Show raw data
        display_cols = ['ts_code', 'name', 'title', 'gender', 'edu', 'lev', 'birthday', 'begin_date', 'end_date', 'national', 'ann_date']
        
        # Rename columns for display
        display_col_rename = {
            'ts_code': '股票代码',
            'name': '姓名',
            'title': '职位',
            'gender': '性别',
            'edu': '教育水平',
            'lev': '层级',
            'birthday': '出生年份',
            'begin_date': '任职开始',
            'end_date': '任职结束',
            'national': '国籍',
            'ann_date': '公告日期'
        }
        
        # Replace gender codes
        df_display = df[display_cols].copy()
        df_display['gender'] = df_display['gender'].map({'M': '男', 'F': '女'}).fillna('未知')
        
        # Rename columns
        df_display = df_display.rename(columns=display_col_rename)
        
        # Show data
        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True
        )
        
        # Add download button
        csv_data = df_display.to_csv(index=False)
        st.download_button(
            label="下载CSV",
            data=csv_data,
            file_name="stock_managers_data.csv",
            mime="text/csv",
        )

# Run the app
if __name__ == "__main__":
    main()