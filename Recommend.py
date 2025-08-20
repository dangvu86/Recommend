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
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    processed_data = output.getvalue()
    return processed_data

def convert_gdrive_link(gdrive_url):
    """
    Chuyển đổi link chia sẻ Google Drive (file hoặc sheet) thành link tải trực tiếp.
    """
    # Kiểm tra định dạng link của Google Sheet
    sheet_match = re.search(r'/spreadsheets/d/([a-zA-Z0-9_-]+)', gdrive_url)
    if sheet_match:
        sheet_id = sheet_match.group(1)
        return f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx'

    # Kiểm tra định dạng link của file thông thường (Excel, Word, etc.)
    file_match = re.search(r'/file/d/([a-zA-Z0-9_-]+)', gdrive_url)
    if file_match:
        file_id = file_match.group(1)
        return f'https://drive.google.com/uc?export=download&id={file_id}'
        
    # Trả về None nếu không có định dạng nào khớp
    return None

def add_performance_cols(df, prices_pivot, date_col_name):
    """
    Thêm các cột hiệu suất vào DataFrame kết quả.
    """
    if df.empty or prices_pivot.empty:
        df['Hiệu suất CP (6T)'] = 'N/A'
        df['Hiệu suất VNINDEX (6T)'] = 'N/A'
        return df

    stock_perfs = []
    vnindex_perfs = []
    vnindex_ticker = 'VNINDEX Index'

    for _, row in df.iterrows():
        stock = row['Cổ phiếu']
        start_date = pd.to_datetime(row[date_col_name])
        end_date = start_date + DateOffset(months=6)

        try:
            if stock not in prices_pivot.columns or vnindex_ticker not in prices_pivot.columns:
                raise KeyError(f"Không tìm thấy mã {stock} hoặc {vnindex_ticker} trong dữ liệu giá.")

            start_prices_slice = prices_pivot.loc[start_date:].dropna(subset=[stock, vnindex_ticker])
            if start_prices_slice.empty:
                raise IndexError("Ngày bắt đầu nằm ngoài phạm vi dữ liệu giá.")
            
            start_price_stock = start_prices_slice[stock].iloc[0]
            start_price_vnindex = start_prices_slice[vnindex_ticker].iloc[0]

            end_prices_slice = prices_pivot.loc[:end_date].dropna(subset=[stock, vnindex_ticker])
            if end_prices_slice.empty:
                raise IndexError("Ngày kết thúc nằm ngoài phạm vi dữ liệu giá.")
                
            end_price_stock = end_prices_slice[stock].iloc[-1]
            end_price_vnindex = end_prices_slice[vnindex_ticker].iloc[-1]

            stock_perf = (end_price_stock / start_price_stock) - 1
            vnindex_perf = (end_price_vnindex / start_price_vnindex) - 1
            
            stock_perfs.append(f"{stock_perf:.2%}")
            vnindex_perfs.append(f"{vnindex_perf:.2%}")

        except (KeyError, IndexError, ValueError):
            stock_perfs.append('N/A')
            vnindex_perfs.append('N/A')

    df['Hiệu suất CP (6T)'] = stock_perfs
    df['Hiệu suất VNINDEX (6T)'] = vnindex_perfs
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

    df_list1 = add_performance_cols(df_list1, prices_pivot, 'Ngày thay đổi')
    df_list2 = add_performance_cols(df_list2, prices_pivot, 'Ngày thay đổi')
    df_list3 = add_performance_cols(df_list3, prices_pivot, 'Ngày khuyến nghị')
    df_list4 = add_performance_cols(df_list4, prices_pivot, 'Ngày khuyến nghị')

    return df_list1, df_list2, df_list3, df_list4

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

                if not all(df.empty for df in [df_list1, df_list2, df_list3, df_list4]):
                    st.success("Xử lý file thành công! Dưới đây là kết quả:")
                    st.header("Kết quả lọc")

                    col1, col2 = st.columns(2)
                    with col1:
                        st.subheader("📉 OUTPERFORM sang MARKET-PERFORM")
                        st.dataframe(df_list1, use_container_width=True, hide_index=True)
                        st.subheader("✅ Khuyến nghị MUA (BUY)")
                        st.dataframe(df_list3, use_container_width=True, hide_index=True)
                    with col2:
                        st.subheader("🚀 MARKET-PERFORM sang OUTPERFORM")
                        st.dataframe(df_list2, use_container_width=True, hide_index=True)
                        st.subheader("⚠️ Khuyến nghị KÉM HIỆU QUẢ (UNDER-PERFORM)")
                        st.dataframe(df_list4, use_container_width=True, hide_index=True)

                    st.divider()
                    st.header("📥 Tải xuống kết quả")
                    dfs_for_export = {
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


# Chạy phân tích tự động
run_analysis(HARCODED_GDRIVE_URL)