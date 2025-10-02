import pandas as pd
import streamlit as st
import duckdb
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode

# ===========================
# PAGE SETUP
# ===========================
st.set_page_config(layout="wide", page_title="🧮 Dashboard Khách hàng")
conn = duckdb.connect(':memory:')

# Header center cho AgGrid
custom_css = {
    ".ag-header-cell-label": {"justify-content": "center"},
    ".ag-header-group-cell-label": {"justify-content": "center"},
}

# JS: số (nghìn) căn phải, 0/None -> rỗng
js_number_right = JsCode("""
function(params) {
  if (params.value === 0 || params.value === null || params.value === undefined || params.value === '') return '';
  let v = params.value;
  if (typeof v === 'string') {
    let num = Number(v.replace(/,/g,''));
    if (!isNaN(num)) return num.toLocaleString('vi-VN');
  }
  if (typeof v === 'number') return v.toLocaleString('vi-VN');
  return v;
}
""")

# JS: số hoặc % căn phải (cho cột "Tỉ lệ")
js_number_or_percent_right = JsCode("""
function(params) {
  if (params.value === 0 || params.value === null || params.value === undefined || params.value === '') return '';
  if (params.colDef.field === 'Tỉ lệ') {
    return (Number(params.value) * 100).toFixed(2) + '%';
  }
  let v = params.value;
  if (typeof v === 'string') {
    let num = Number(v.replace(/,/g,''));
    if (!isNaN(num)) return num.toLocaleString('vi-VN');
  }
  if (typeof v === 'number') return v.toLocaleString('vi-VN');
  return v;
}
""")

# JS: style highlight max (nhưng vẫn right align)
js_highlight_max_tpl = """
function(params) {{
  if (params.value === {max_val}) {{
    return {{ backgroundColor: 'lightgreen', textAlign: 'right' }};
  }}
  return {{ textAlign: 'right' }};
}}
"""

# JS: render thay đổi (±, màu) + căn phải
js_change_renderer = JsCode("""
function(params) {
  if (params.value === null || params.value === undefined || params.value === '') return '';
  let v = params.value;
  let num = Number(String(v).replace(/,/g,'')); // robust parse
  if (isNaN(num)) return v;
  let sign = (num > 0 ? '+' : '');
  let txt = sign + Math.abs(num).toLocaleString('vi-VN');
  let color = (num > 0 ? 'green' : (num < 0 ? 'red' : ''));
  return `<span style="color:${color}; float:right;">${sign === '+' ? '+' : (num < 0 ? '-' : '')}${Math.abs(num).toLocaleString('vi-VN')}</span>`;
}
""")

# ===========================
# LOAD DATA
# ===========================
sheet_id = "1N5Len0S4vxZrzksnZJDImF6rK6--G8YEMPLbbYmmKvs"

gid1 = "1961129161"
url1 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid1}"
key_in = pd.read_csv(url1, skiprows=1, header=0, usecols=range(36))

gid3 = "782116804"
url3 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid3}"
NAV_batch = pd.read_csv(url3, skiprows=0, header=0, usecols=range(7))

# rename
key_in.columns.values[1]  = "khach_hang"
key_in.columns.values[6]  = "ma"
key_in.columns.values[8]  = "so_luong_mua"
key_in.columns.values[10] = "on_off"
key_in.columns.values[18] = "tien_ban_phi"
key_in.columns.values[20] = "du_no_hien_tai"
key_in.columns.values[24] = "lai_lo_sau_cung"

NAV_batch.columns.values[0] = "khach_hang"
NAV_batch.columns.values[5] = "lai_vay_ngay"
NAV_batch.columns.values[6] = "ngay"

# types
cols_num = ['du_no_hien_tai','so_luong_mua','lai_lo_sau_cung','tien_ban_phi','NAV']
key_in[cols_num] = key_in[cols_num].replace(',', '', regex=True).apply(pd.to_numeric, errors='coerce')
NAV_batch['lai_vay_ngay'] = pd.to_numeric(NAV_batch['lai_vay_ngay'].astype(str).str.replace(',','', regex=False), errors='coerce')
NAV_batch['ngay'] = pd.to_datetime(NAV_batch['ngay'], errors='coerce')

# register
conn.register('key_in', key_in)
conn.register('NAV_batch', NAV_batch)

st.title("🧮 Dashboard Khách hàng")
st.markdown("<br>", unsafe_allow_html=True)

# ===========================
# 1) NAV NGÀY
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
  from key_in where on_off='ON' group by khach_hang
) b on a.khach_hang=b.khach_hang
'''
nav = conn.execute(query1).fetchdf().rename(columns={
    'khach_hang':'Khách hàng',
    'lai_lo_sau_cung':'Lãi lỗ sau cùng',
    'du_no_hien_tai':'Dư nợ hiện tại',
    'gia_tri_danh_muc':'Giá trị danh mục',
    'ti_le':'Tỉ lệ'
})
for c in ['NAV','Lãi lỗ sau cùng','Dư nợ hiện tại','Giá trị danh mục','Tỉ lệ']:
    nav[c] = pd.to_numeric(nav[c], errors='coerce').fillna(0)
max_vals = {c: nav[c].max() for c in ['NAV','Lãi lỗ sau cùng','Dư nợ hiện tại','Giá trị danh mục','Tỉ lệ']}

gb1 = GridOptionsBuilder.from_dataframe(nav)
gb1.configure_default_column(resizable=True, filter=True, headerClass='centered', cellStyle={'textAlign':'center'})
gb1.configure_column('Khách hàng', pinned='left', width=170, cellStyle={'textAlign':'center'}, headerClass='centered')
for col in ['NAV','Lãi lỗ sau cùng','Dư nợ hiện tại','Giá trị danh mục','Tỉ lệ']:
    js_style = JsCode(js_highlight_max_tpl.format(max_val=max_vals[col]))
    gb1.configure_column(col, cellRenderer=js_number_or_percent_right, cellStyle=js_style, width=140, headerClass='centered')

st.header("📈 NAV ngày")
AgGrid(nav, gridOptions=gb1.build(), custom_css=custom_css, fit_columns_on_grid_load=True,
       height=450, theme='streamlit', allow_unsafe_jscode=True)

st.markdown("<br>", unsafe_allow_html=True)

# ===========================
# 2) SỐ LƯỢNG MUA (có 'Tổng' ở dưới theo CỘT)
# ===========================
st.header("🛒 Số lượng mua")
df2 = conn.execute("""
select khach_hang, ma, so_luong_mua
from key_in where on_off='ON' and so_luong_mua != 0 and length(ma)=3
""").fetchdf()

pivot_buy = pd.pivot_table(df2, values='so_luong_mua', index='khach_hang',
                           columns='ma', aggfunc='sum', fill_value=0)
# Dòng Tổng theo cột ở CUỐI bảng
total_row = pivot_buy.sum(axis=0)
total_row.name = 'Tổng'
pivot_buy = pd.concat([pivot_buy, pd.DataFrame([total_row])])

pivot_buy = pivot_buy.reset_index().rename(columns={'khach_hang':'Khách hàng'})

gb2 = GridOptionsBuilder.from_dataframe(pivot_buy)
gb2.configure_default_column(resizable=True, headerClass='centered', cellStyle={'textAlign':'right'})
gb2.configure_column('Khách hàng', pinned='left', min_width=180, cellStyle={'textAlign':'center'}, headerClass='centered')
for c in pivot_buy.columns:
    if c != 'Khách hàng':
        gb2.configure_column(c, cellRenderer=js_number_right, min_width=100, headerClass='centered')

AgGrid(pivot_buy, gridOptions=gb2.build(), custom_css=custom_css, fit_columns_on_grid_load=True,
       height=560, theme='streamlit', allow_unsafe_jscode=True)

st.markdown("<br>", unsafe_allow_html=True)

# ===========================
# 3) LÃI VAY THEO NGÀY (có cột "(thay đổi)")
# ===========================
st.header("💰 Lãi vay theo ngày")

lai = conn.execute("select khach_hang, ngay, lai_vay_ngay from NAV_batch").fetchdf()
pivot_loan = pd.pivot_table(lai, values='lai_vay_ngay', index='khach_hang', columns='ngay',
                            aggfunc='sum', fill_value=0)

# 1) Cột ngày ASC để tính diff: diff(d) = value(d) - value(prev)
pivot_loan = pivot_loan.sort_index(axis=1)
diff_loan = pivot_loan.diff(axis=1)

# 2) Tạo bảng "giá trị + (thay đổi)" theo THỨ TỰ MỚI NHẤT → CŨ NHẤT
dates_asc = list(pivot_loan.columns)
dates_desc = list(reversed(dates_asc))

cols_out = []
for d in dates_desc:
    ds = d.strftime("%d/%m/%Y")
    cols_out.append(ds)  # giá trị ngày d
    if d != dates_asc[0]:  # không có thay đổi cho ngày cổ nhất
        cols_out.append(f"{ds} (thay đổi)")

combined = pd.DataFrame(index=pivot_loan.index, columns=cols_out)

for d in dates_desc:
    ds = d.strftime("%d/%m/%Y")
    combined[ds] = pivot_loan[d]
    if d != dates_asc[0]:
        # diff tại ngày d so với ngày trước đó (theo ASC)
        combined[f"{ds} (thay đổi)"] = diff_loan[d]

# Đưa index thành cột
combined = combined.reset_index().rename(columns={'khach_hang':'khach_hang'})
combined['khach_hang'] = combined['khach_hang'].astype(str)

gb3 = GridOptionsBuilder.from_dataframe(combined)
# Header center, chữ center; số right
gb3.configure_default_column(resizable=True, headerClass='centered',
                             cellStyle={'textAlign':'right'})  # default right
gb3.configure_column('khach_hang', header_name='Khách hàng',
                     pinned='left', min_width=180,
                     cellStyle={'textAlign':'center'}, headerClass='centered')

for col in combined.columns:
    if col == 'khach_hang':
        continue
    if '(thay đổi)' in col:
        # renderer đổi màu ± và right align
        gb3.configure_column(col, cellRenderer=js_change_renderer, min_width=120, headerClass='centered')
    else:
        gb3.configure_column(col, cellRenderer=js_number_right, min_width=110, headerClass='centered')

AgGrid(combined, gridOptions=gb3.build(), custom_css=custom_css, fit_columns_on_grid_load=False,
       height=620, theme='streamlit', allow_unsafe_jscode=True)

st.markdown("<br>", unsafe_allow_html=True)

# ===========================
# 4) LINE CHART: TỔNG LÃI VAY THEO NGÀY
# ===========================
st.header("📊 Tổng lãi vay theo ngày (Line)")
sum_by_day = conn.execute("""
select ngay, sum(lai_vay_ngay) as tong
from NAV_batch
group by ngay
order by ngay
""").fetchdf().set_index('ngay')

st.line_chart(sum_by_day['tong'])
