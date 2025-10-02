import pandas as pd
import streamlit as st
import duckdb
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode

# ===========================
# 1) PAGE SETUP
# ===========================
st.set_page_config(layout="wide", page_title="🧮 Dashboard Khách hàng")
conn = duckdb.connect(':memory:')

# Header/text center, numbers right – dùng chung cho mọi bảng
custom_css = {
    ".ag-header-cell-label": {"justify-content": "center"},
    ".ag-header-group-cell-label": {"justify-content": "center"},
}

# JS: format số và canh phải
js_number_right = JsCode("""
function(params) {
  if (params.value === 0 || params.value === null || params.value === undefined || params.value === '') return '';
  let v = params.value;
  if (typeof v === 'string') {
    let num = Number(v.replace(/,/g, ''));
    if (!isNaN(num)) return num.toLocaleString('vi-VN');
  }
  if (typeof v === 'number') return v.toLocaleString('vi-VN');
  return v;
}
""")

# ===========================
# 2) DATA
# ===========================
sheet_id = "1N5Len0S4vxZrzksnZJDImF6rK6--G8YEMPLbbYmmKvs"

gid1 = "1961129161"
url1 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid1}"
key_in = pd.read_csv(url1, skiprows=1, header=0, usecols=range(36))

gid3 = "782116804"
url3 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid3}"
NAV_batch = pd.read_csv(url3, skiprows=0, header=0, usecols=range(7))

# Đổi tên cột
key_in.columns.values[1] = "khach_hang"
key_in.columns.values[6] = "ma"
key_in.columns.values[8] = "so_luong_mua"
key_in.columns.values[10] = "on_off"
key_in.columns.values[18] = "tien_ban_phi"
key_in.columns.values[20] = "du_no_hien_tai"
key_in.columns.values[24] = "lai_lo_sau_cung"

NAV_batch.columns.values[0] = "khach_hang"
NAV_batch.columns.values[5] = "lai_vay_ngay"
NAV_batch.columns.values[6] = "ngay"

# Kiểu dữ liệu
cols_num = ['du_no_hien_tai', 'so_luong_mua', 'lai_lo_sau_cung', 'tien_ban_phi', 'NAV']
key_in[cols_num] = key_in[cols_num].replace(',', '', regex=True).apply(pd.to_numeric, errors='coerce')
NAV_batch['lai_vay_ngay'] = pd.to_numeric(NAV_batch['lai_vay_ngay'].astype(str).str.replace(',', '', regex=False), errors='coerce')
NAV_batch['ngay'] = pd.to_datetime(NAV_batch['ngay'], errors='coerce')

# Đăng ký vào DuckDB
conn.register('key_in', key_in)
conn.register('NAV_batch', NAV_batch)

st.title("🧮 Dashboard Khách hàng")
st.markdown("<br>", unsafe_allow_html=True)

# ===========================
# 3) NAV NGÀY (text center, số right)
# ===========================
query1 = '''
select a.*,
       b.lai_lo_sau_cung,
       b.du_no_hien_tai,
       b.gia_tri_danh_muc,
       case when b.gia_tri_danh_muc = 0 then null else a.NAV/b.gia_tri_danh_muc end as ti_le
from (select khach_hang, sum(NAV) as NAV from key_in where khach_hang is not null group by khach_hang) a
left join (
  select khach_hang,
         sum(lai_lo_sau_cung) as lai_lo_sau_cung,
         sum(du_no_hien_tai)  as du_no_hien_tai,
         sum(tien_ban_phi)    as gia_tri_danh_muc
  from key_in
  where on_off='ON'
  group by khach_hang
) b on a.khach_hang = b.khach_hang
'''
nav_daily = conn.execute(query1).fetchdf().rename(columns={
    'khach_hang': 'Khách hàng',
    'lai_lo_sau_cung': 'Lãi lỗ sau cùng',
    'du_no_hien_tai': 'Dư nợ hiện tại',
    'gia_tri_danh_muc': 'Giá trị danh mục',
    'ti_le': 'Tỉ lệ'
})
for c in ['NAV', 'Lãi lỗ sau cùng', 'Dư nợ hiện tại', 'Giá trị danh mục', 'Tỉ lệ']:
    nav_daily[c] = pd.to_numeric(nav_daily[c], errors='coerce').fillna(0)

# Highlight max (nhưng vẫn right align)
js_highlight_max = """
function(params) {{
  if (params.value === {max_val}) {{
    return {{ backgroundColor: 'lightgreen', textAlign: 'right' }};
  }}
  return {{ textAlign: 'right' }};
}}
"""
max_vals = {c: nav_daily[c].max() for c in ['NAV', 'Lãi lỗ sau cùng', 'Dư nợ hiện tại', 'Giá trị danh mục', 'Tỉ lệ']}

gb1 = GridOptionsBuilder.from_dataframe(nav_daily)
gb1.configure_default_column(resizable=True, filter=True, headerClass='centered-header', cellStyle={'textAlign': 'center'})
gb1.configure_column('Khách hàng', pinned='left', width=170, cellStyle={'textAlign': 'center'})
for col in ['NAV', 'Lãi lỗ sau cùng', 'Dư nợ hiện tại', 'Giá trị danh mục', 'Tỉ lệ']:
    js_style = JsCode(js_highlight_max.format(max_val=max_vals[col]))
    gb1.configure_column(col, cellRenderer=js_number_right, cellStyle=js_style, width=140, headerClass='centered-header')

st.header("📈 NAV ngày")
AgGrid(
    nav_daily,
    gridOptions=gb1.build(),
    custom_css=custom_css,
    fit_columns_on_grid_load=True,
    height=450,
    theme='streamlit',
    allow_unsafe_jscode=True
)

st.markdown("<br>", unsafe_allow_html=True)

# ===========================
# 4) SỐ LƯỢNG MUA (thêm dòng Tổng ở dưới)
# ===========================
st.header("🛒 Số lượng mua")

query2 = '''
select khach_hang, ma, so_luong_mua
from key_in
where on_off='ON' and so_luong_mua != 0 and length(ma)=3
'''
df2 = conn.execute(query2).fetchdf()

pivot = pd.pivot_table(df2, values='so_luong_mua', index='khach_hang', columns='ma', aggfunc='sum', fill_value=0)
# Thêm dòng tổng theo cột (ở CUỐI bảng)
total_row = pivot.sum(axis=0)
total_row.name = 'Tổng'
pivot = pd.concat([pivot, pd.DataFrame([total_row])])

pivot = pivot.reset_index().rename(columns={'khach_hang': 'Khách hàng'})

gb2 = GridOptionsBuilder.from_dataframe(pivot)
gb2.configure_default_column(resizable=True, headerClass='centered-header', cellStyle={'textAlign': 'right'})
gb2.configure_column('Khách hàng', pinned='left', min_width=180, cellStyle={'textAlign': 'center'}, headerClass='centered-header')
for c in pivot.columns:
    if c != 'Khách hàng':
        gb2.configure_column(c, cellRenderer=js_number_right, min_width=100, headerClass='centered-header')

AgGrid(
    pivot,
    gridOptions=gb2.build(),
    custom_css=custom_css,
    fit_columns_on_grid_load=True,
    height=560,
    theme='streamlit',
    allow_unsafe_jscode=True
)

st.markdown("<br>", unsafe_allow_html=True)

# ===========================
# 5) LÃI VAY THEO NGÀY (bảng)
# ===========================
st.header("💰 Lãi vay theo ngày")

query3 = "select khach_hang, ngay, lai_vay_ngay from NAV_batch"
lai = conn.execute(query3).fetchdf()

pivot3 = pd.pivot_table(lai, values='lai_vay_ngay', index='khach_hang', columns='ngay', aggfunc='sum', fill_value=0)
pivot3 = pivot3.sort_index(axis=1)  # cột theo thời gian tăng dần
pivot3.columns = [d.strftime('%d/%m/%Y') for d in pivot3.columns]
pivot3 = pivot3.reset_index().rename(columns={'khach_hang': 'Khách hàng'})

gb3 = GridOptionsBuilder.from_dataframe(pivot3)
gb3.configure_default_column(resizable=True, headerClass='centered-header', cellStyle={'textAlign': 'right'})
gb3.configure_column('Khách hàng', pinned='left', min_width=180, cellStyle={'textAlign': 'center'}, headerClass='centered-header')
for c in pivot3.columns:
    if c != 'Khách hàng':
        gb3.configure_column(c, cellRenderer=js_number_right, min_width=110, headerClass='centered-header')

AgGrid(
    pivot3,
    gridOptions=gb3.build(),
    custom_css=custom_css,
    fit_columns_on_grid_load=False,
    height=600,
    theme='streamlit',
    allow_unsafe_jscode=True
)

st.markdown("<br>", unsafe_allow_html=True)

# ===========================
# 6) BIỂU ĐỒ ĐƯỜNG: TỔNG LÃI VAY THEO NGÀY
# ===========================
st.header("📊 Tổng lãi vay theo ngày (Line)")

query4 = "select ngay, sum(lai_vay_ngay) as tong from NAV_batch group by ngay order by ngay"
tong = conn.execute(query4).fetchdf().set_index('ngay')

st.line_chart(tong['tong'])
