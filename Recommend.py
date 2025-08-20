import streamlit as st
import pandas as pd
import io
import re
from pandas.tseries.offsets import DateOffset

# --- Cấu hình ---
# Dán link chia sẻ file Google Drive của bạn vào đây
HARCODED_GDRIVE_URL = "https://docs.google.com/spreadsheets/d/18lAJxn-Uy1pNLc6qAxJAoDhZXEBBbsaJ/edit?usp=drive_link&ouid=109054371302579758735&rtpof=true&sd=true"

def to_excel(dfs_dict):
    """
    Hàm chuyển đổi một từ điển chứa các DataFrame thành một file Excel trong bộ nhớ.
    Mỗi cặp key-value trong từ điển sẽ tương ứng với một sheet trong file Excel.
    """
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        for sheet_name, df in dfs_dict.items():
            # Nếu df là Styler object, lấy data ra
            if hasattr(df, 'data'):
                df.data.to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                df.to_excel(writer, sheet_name=sheet_name, index=False)
    processed_data = output.getvalue()
    return processed_data

def convert_gdrive_link(gdrive_url):
    """
    Chuyển đổi link chia sẻ Google Drive (file hoặc sheet) thành link tải trực tiếp.
    """
    sheet_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', gdrive_url)
    if sheet_match:
        sheet_id = sheet_match.group(1)
        return f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx'

    file_match = re.search(r'/file/d/([a-zA-Z0-9_-]+)', gdrive_url)
    if file_match:
        file_id = file_match.group(1)
        return f'https://drive.google.com/uc?export=download&id={file_id}'
        
    return None

def add_performance_cols(df, prices_pivot, date_col_name):
    """
    Thêm các cột hiệu suất và rating vào DataFrame kết quả.
    """
    if df.empty or prices_pivot.empty:
        df['Hiệu suất CP (6T)'] = 'N/A'
        df['Hiệu suất VNINDEX (6T)'] = 'N/A'
        df['vs VNINDEX (6T)'] = 'N/A'
        df['Rating'] = 'N/A'
        return df

    stock_perfs, vnindex_perfs, vs_vnindex_perfs, ratings = [], [], [], []
    vnindex_ticker = 'VNINDEX Index'

    for _, row in df.iterrows():
        stock = row['Cổ phiếu']
        start_date = pd.to_datetime(row[date_col_name])
        end_date = start_date + DateOffset(months=6)

        try:
            if stock not in prices_pivot.columns or vnindex_ticker not in prices_pivot.columns:
                raise KeyError(f"Không tìm thấy mã {stock} hoặc {vnindex_ticker} trong dữ liệu giá.")

            start_prices_slice = prices_pivot.loc[start_date:].dropna(subset=[stock, vnindex_ticker])
            if start_prices_slice.empty: raise IndexError("Ngày bắt đầu nằm ngoài phạm vi.")
            
            start_price_stock = start_prices_slice[stock].iloc[0]
            start_price_vnindex = start_prices_slice[vnindex_ticker].iloc[0]

            end_prices_slice = prices_pivot.loc[:end_date].dropna(subset=[stock, vnindex_ticker])
            if end_prices_slice.empty: raise IndexError("Ngày kết thúc nằm ngoài phạm vi.")
                
            end_price_stock = end_prices_slice[stock].iloc[-1]
            end_price_vnindex = end_prices_slice[vnindex_ticker].iloc[-1]

            stock_perf_num = (end_price_stock / start_price_stock) - 1
            vnindex_perf_num = (end_price_vnindex / start_price_vnindex) - 1
            vs_vnindex_perf_num = stock_perf_num - vnindex_perf_num
            
            stock_perfs.append(f"{stock_perf_num:.2%}")
            vnindex_perfs.append(f"{vnindex_perf_num:.2%}")
            vs_vnindex_perfs.append(f"{vs_vnindex_perf_num:.2%}")

            if vs_vnindex_perf_num > 0:
                ratings.append('Outperform')
            elif vs_vnindex_perf_num < 0:
                ratings.append('Underperform')
            else: 
                ratings.append('N/A')

        except (KeyError, IndexError, ValueError):
            stock_perfs.append('N/A')
            vnindex_perfs.append('N/A')
            vs_vnindex_perfs.append('N/A')
            ratings.append('N/A')

    df['Hiệu suất CP (6T)'] = stock_perfs
    df['Hiệu suất VNINDEX (6T)'] = vnindex_perfs
    df['vs VNINDEX (6T)'] = vs_vnindex_perfs
    df['Rating'] = ratings
    return df

def process_stock_data(df_rec, df_price):
    """
    Hàm xử lý, làm sạch và phân tích dữ liệu cổ phiếu từ DataFrame.
    """
    df_rec.dropna(axis=1, how='all', inplace=True)
    df_rec = df_rec.loc[:, ~df_rec.columns.str.contains('^Unnamed')]
    df_rec.index = pd.to_datetime(df_rec.index, errors='coerce')
    df_rec = df_rec[df_rec.index.notna()]

    if df_rec.empty:
        st.warning("Không tìm thấy dữ liệu ngày tháng hợp lệ trong sheet khuyến nghị.")
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

    df_rec.dropna(axis=0, how='all', inplace=True)
    df_rec.sort_index(inplace=True)
    
    df_filled = df_rec.ffill()

    prices_pivot = pd.DataFrame()
    if not df_price.empty:
        df_price['Date'] = pd.to_datetime(df_price['Date'], errors='coerce')
        df_price.dropna(subset=['Date'], inplace=True)
        prices_pivot = df_price.pivot_table(index='Date', columns='Stock', values='Price', aggfunc='mean')
        prices_pivot.sort_index(inplace=True)

    df_list1 = pd.DataFrame(columns=['Cổ phiếu', 'Ngày thay đổi'])
    df_list2 = pd.DataFrame(columns=['Cổ phiếu', 'Ngày thay đổi'])
    
    if len(df_filled) >= 2:
        df_shifted = df_filled.shift(1)
        cond1 = (df_filled == 'MARKET-PERFORM') & (df_shifted == 'OUTPERFORM')
        list1_data = [{'Cổ phiếu': stock, 'Ngày thay đổi': date.strftime('%Y-%m-%d')} for stock in cond1.columns for date in cond1.index[cond1[stock]]]
        if list1_data: df_list1 = pd.DataFrame(list1_data)

        cond2 = (df_filled == 'OUTPERFORM') & (df_shifted == 'MARKET-PERFORM')
        list2_data = [{'Cổ phiếu': stock, 'Ngày thay đổi': date.strftime('%Y-%m-%d')} for stock in cond2.columns for date in cond2.index[cond2[stock]]]
        if list2_data: df_list2 = pd.DataFrame(list2_data)

    buy_data = [{'Cổ phiếu': stock, 'Ngày khuyến nghị': date.strftime('%Y-%m-%d')} for stock in df_rec.columns for date in df_rec.index[df_rec[stock] == 'BUY']]
    df_list3 = pd.DataFrame(buy_data) if buy_data else pd.DataFrame(columns=['Cổ phiếu', 'Ngày khuyến nghị'])
    
    under_data = [{'Cổ phiếu': stock, 'Ngày khuyến nghị': date.strftime('%Y-%m-%d')} for stock in df_rec.columns for date in df_rec.index[df_rec[stock] == 'UNDER-PERFORM']]
    df_list4 = pd.DataFrame(under_data) if under_data else pd.DataFrame(columns=['Cổ phiếu', 'Ngày khuyến nghị'])

    # **FIX:** Sắp xếp các bảng theo ngày giảm dần
    if not df_list1.empty: df_list1 = df_list1.sort_values(by='Ngày thay đổi', ascending=False)
    if not df_list2.empty: df_list2 = df_list2.sort_values(by='Ngày thay đổi', ascending=False)
    if not df_list3.empty: df_list3 = df_list3.sort_values(by='Ngày khuyến nghị', ascending=False)
    if not df_list4.empty: df_list4 = df_list4.sort_values(by='Ngày khuyến nghị', ascending=False)

    df_list1 = add_performance_cols(df_list1, prices_pivot, 'Ngày thay đổi')
    df_list2 = add_performance_cols(df_list2, prices_pivot, 'Ngày thay đổi')
    df_list3 = add_performance_cols(df_list3, prices_pivot, 'Ngày khuyến nghị')
    df_list4 = add_performance_cols(df_list4, prices_pivot, 'Ngày khuyến nghị')

    return df_list1, df_list2, df_list3, df_list4

def calculate_win_rate_summary(df1, df2, df3, df4):
    """
    Tạo bảng thống kê Win Rate theo năm.
    """
    # Làm việc trên bản sao để không ảnh hưởng đến DataFrame gốc
    dfs = {
        'OUTPERFORM sang MARKET-PERFORM': (df1.copy(), 'Ngày thay đổi'),
        'MARKET-PERFORM sang OUTPERFORM': (df2.copy(), 'Ngày thay đổi'),
        'Khuyến nghị BUY': (df3.copy(), 'Ngày khuyến nghị'),
        'Khuyến nghị UNDER-PERFORM': (df4.copy(), 'Ngày khuyến nghị')
    }

    all_years = set()
    for _, (df, date_col) in dfs.items():
        if not df.empty and date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            valid_dates = df.dropna(subset=[date_col])
            if not valid_dates.empty:
                all_years.update(valid_dates[date_col].dt.year.unique())

    if not all_years:
        return pd.DataFrame()

    years = sorted(list(all_years))
    summary_data = {}

    for col_name, (df, date_col) in dfs.items():
        win_rates = []
        if not df.empty and 'Rating' in df.columns:
            df_filtered = df[df['Rating'] != 'N/A'].copy()
            df_filtered['Year'] = pd.to_datetime(df_filtered[date_col]).dt.year

            for year in years:
                year_df = df_filtered[df_filtered['Year'] == year]
                total_cases = len(year_df)
                if total_cases > 0:
                    win_cases = len(year_df[year_df['Rating'] == 'Outperform'])
                    win_rate = win_cases / total_cases
                    win_rates.append(f"{win_rate:.2%} ({win_cases}/{total_cases})")
                else:
                    win_rates.append('N/A')
            
            total_all_time = len(df_filtered)
            if total_all_time > 0:
                win_all_time = len(df_filtered[df_filtered['Rating'] == 'Outperform'])
                total_win_rate = win_all_time / total_all_time
                win_rates.append(f"{total_win_rate:.2%} ({win_all_time}/{total_all_time})")
            else:
                win_rates.append('N/A')
            summary_data[col_name] = win_rates
        else:
            summary_data[col_name] = ['N/A'] * (len(years) + 1)

    summary_df = pd.DataFrame(summary_data, index=[str(y) for y in years] + ['Total'])
    summary_df.index.name = 'Win Rate'
    return summary_df.reset_index()

def run_analysis(gdrive_url):
    """
    Hàm chính để chạy toàn bộ quy trình phân tích.
    """
    if gdrive_url and gdrive_url != "YOUR_GOOGLE_DRIVE_LINK_HERE":
        with st.spinner("Đang tải và xử lý dữ liệu..."):
            try:
                download_url = convert_gdrive_link(gdrive_url)
                if download_url is None:
                    st.error("Link Google Drive không hợp lệ. Vui lòng kiểm tra lại link trong code.")
                    return

                xls = pd.ExcelFile(download_url)
                if 'Sheet1' not in xls.sheet_names or 'Price' not in xls.sheet_names:
                    st.error("Lỗi: File Excel phải chứa cả hai sheet tên là 'Sheet1' và 'Price'.")
                    return
                
                df_rec = pd.read_excel(xls, sheet_name='Sheet1', header=1, index_col=0)
                df_price = pd.read_excel(xls, sheet_name='Price')
                
                df_list1, df_list2, df_list3, df_list4 = process_stock_data(df_rec, df_price)

                # --- Định dạng bảng ---
                def style_rating(val):
                    color = ''
                    if val == 'Outperform': color = '#D4EDDA'
                    elif val == 'Underperform': color = '#F8D7DA'
                    return f'background-color: {color}'

                def style_win_rate(val):
                    color = ''
                    if isinstance(val, str) and '%' in val:
                        try:
                            percent_str = val.split(' ')[0]
                            num_val = float(percent_str.strip('%'))
                            if num_val > 50: color = '#D4EDDA'
                            elif num_val < 50: color = '#F8D7DA'
                        except (ValueError, TypeError): pass
                    return f'background-color: {color}'

                def apply_styles(df):
                    numeric_cols = ['Hiệu suất CP (6T)', 'Hiệu suất VNINDEX (6T)', 'vs VNINDEX (6T)']
                    styler = df.style.map(style_rating, subset=['Rating'])
                    
                    format_dict = {}
                    for col in numeric_cols:
                        if col in df.columns:
                            format_dict[col] = '{: >}'
                    
                    styler = styler.set_properties(**{'text-align': 'right'}, subset=numeric_cols)
                    return styler

                if not all(df.empty for df in [df_list1, df_list2, df_list3, df_list4]):
                    st.success("Xử lý file thành công! Dưới đây là kết quả:")
                    
                    st.header("📊 Bảng tổng hợp Win Rate")
                    summary_df = calculate_win_rate_summary(df_list1, df_list2, df_list3, df_list4)
                    win_rate_cols = summary_df.columns.drop('Win Rate')
                    summary_styler = summary_df.style.apply(lambda x: x.map(style_win_rate), subset=win_rate_cols)
                    st.dataframe(summary_styler, use_container_width=True, hide_index=True)

                    st.header("Kết quả lọc chi tiết")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.subheader("📉 OUTPERFORM sang MARKET-PERFORM")
                        st.dataframe(apply_styles(df_list1), use_container_width=True, hide_index=True)
                        st.subheader("✅ Khuyến nghị MUA (BUY)")
                        st.dataframe(apply_styles(df_list3), use_container_width=True, hide_index=True)
                    with col2:
                        st.subheader("🚀 MARKET-PERFORM sang OUTPERFORM")
                        st.dataframe(apply_styles(df_list2), use_container_width=True, hide_index=True)
                        st.subheader("⚠️ Khuyến nghị KÉM HIỆU QUẢ (UNDER-PERFORM)")
                        st.dataframe(apply_styles(df_list4), use_container_width=True, hide_index=True)

                    st.divider()
                    st.header("📥 Tải xuống kết quả")
                    dfs_for_export = {
                        "Thong_ke_Win_Rate": summary_df,
                        "Out_sang_MarketPerform": df_list1,
                        "MarketPerform_sang_Out": df_list2,
                        "Khuyen_nghi_BUY": df_list3,
                        "Khuyen_nghi_UnderPerform": df_list4
                    }
                    excel_data = to_excel(dfs_for_export)
                    st.download_button(
                        label="📁 Tải file Excel",
                        data=excel_data,
                        file_name="ket_qua_loc_co_phieu.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            except Exception as e:
                st.error(f"Đã xảy ra lỗi khi tải hoặc xử lý file: {e}")
                st.error("Vui lòng đảm bảo link của bạn được chia sẻ ở chế độ 'Bất kỳ ai có đường liên kết'.")
    else:
        st.info("Chào mừng! Vui lòng chỉnh sửa code và thêm link Google Drive vào biến 'HARCODED_GDRIVE_URL' để bắt đầu.")

# --- Giao diện ứng dụng Streamlit ---
st.set_page_config(layout="wide", page_title="Bộ lọc Cổ phiếu")

st.title("📈 Bộ lọc Cổ phiếu theo Khuyến nghị và Hiệu suất")


run_analysis(HARCODED_GDRIVE_URL)
