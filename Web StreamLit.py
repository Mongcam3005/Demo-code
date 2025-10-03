import pandas as pd
import streamlit as st 
import duckdb
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode

st.set_page_config(layout="wide", page_title="🧮 Dashboard Khách hàng")

# ====== AgGrid: header căn giữa dùng chung ======
custom_css = {
    ".ag-header-cell-label": {"justify-content": "center"},
    ".ag-header-group-cell-label": {"justify-content": "center"},
}

# ====== JS render số: căn phải, format nghìn ======
js_number_right = JsCode("""
function(params) {
  if (params.value === null || params.value === undefined || params.value === '') return '0';
  let v = params.value;
  if (typeof v === 'string') {
    let num = Number(v.replace(/,/g,''));
    if (!isNaN(num)) return num.toLocaleString('vi-VN');
  }
  if (typeof v === 'number') return v.toLocaleString('vi-VN');
  return v;
}
""")

# ====== JS render số/percent cho cột 'Tỉ lệ' ======
js_number_or_percent_right = JsCode("""
function(params) {
  if (params.value === null || params.value === undefined || params.value === '') return '0';
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

# ====== JS style highlight max theo cột (vẫn right align) ======
js_highlight_max_tpl = """
function(params) {{
  if (params.value === {max_val}) {{
    return {{ backgroundColor: 'lightgreen', textAlign: 'right' }};
  }}
  return {{ textAlign: 'right' }};
}}
"""

# ====== JS formatter cho cột "(thay đổi)" ======
js_change_valuefmt = JsCode("""
function(params) {
  if (params.value === null || params.value === undefined || params.value === '') return '0';
  let num = Number(String(params.value).replace(/,/g,''));
  if (isNaN(num)) return '0';
  let sign = (num > 0 ? '+' : (num < 0 ? '-' : ''));
  let absval = Math.abs(num).toLocaleString('vi-VN');
  return (sign ? sign : '') + absval;
}
""")

js_change_style = JsCode("""
function(params) {
  if (params.value === null || params.value === undefined || params.value === '') return {textAlign:'right'};
  let num = Number(String(params.value).replace(/,/g,''));
  if (isNaN(num)) return {textAlign:'right'};
  if (num > 0) return {color:'green', textAlign:'right'};
  if (num < 0) return {color:'red', textAlign:'right'};
  return {textAlign:'right'};
}
""")

# ===========================
# DATA
# ===========================
conn = duckdb.connect(':memory:')

sheet_id = "1N5Len0S4vxZrzksnZJDImF6rK6--G8YEMPLbbYmmKvs"

gid1 = "1961129161"
url1 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid1}"
key_in = pd.read_csv(url1, skiprows=1, header=0, usecols=range(36))

gid3 = "782116804"
url3 = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid3}"
NAV_batch = pd.read_csv(url3, skiprows=0, header=0, usecols=range(7))

# Đổi tên cột
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

# Kiểu dữ liệu
cols_can_chuyen = ['du_no_hien_tai', 'so_luong_mua','lai_lo_sau_cung','tien_ban_phi','NAV']
key_in[cols_can_chuyen] = key_in[cols_can_chuyen].replace(',', '', regex=True).apply(pd.to_numeric, errors='coerce')
NAV_batch['lai_vay_ngay'] = pd.to_numeric(NAV_batch['lai_vay_ngay'].astype(str).str.replace(',', '', regex=False), errors='coerce')
NAV_batch['ngay'] = pd.to_datetime(NAV_batch['ngay'],errors='coerce')

# Đăng ký
conn.register('key_in',key_in)
conn.register('NAV_batch',NAV_batch)

# ===========================
# UI
# ===========================
st.title('🧮 Dashboard Khách hàng')
st.markdown("<br>", unsafe_allow_html=True)

# ===========================
# 1. NAV NGÀY  (ẩn nguyên hàng nếu |NAV| < 100,000)
# ===========================
query1 ='''
select a.*, 
       b.lai_lo_sau_cung, 
       b.du_no_hien_tai,
       b.gia_tri_danh_muc,
       case when b.gia_tri_danh_muc = 0 then null else a.NAV/b.gia_tri_danh_muc end as ti_le
from (select khach_hang, sum(NAV) as NAV
      from key_in
      where khach_hang is not null
      group by khach_hang) a
left join
     (select khach_hang, 
             sum(lai_lo_sau_cung) as lai_lo_sau_cung,
             sum(du_no_hien_tai)   as du_no_hien_tai,
             sum(tien_ban_phi)     as gia_tri_danh_muc
      from key_in
      where on_off = 'ON'
      group by khach_hang) b
on a.khach_hang = b.khach_hang
'''
nav_daily = conn.execute(query1).fetchdf()

nav_daily = nav_daily.rename(columns={
    'khach_hang' : 'Khách hàng',
    'lai_lo_sau_cung': 'Lãi lỗ sau cùng',
    'du_no_hien_tai': 'Dư nợ hiện tại',
    'gia_tri_danh_muc': 'Giá trị danh mục',
    'ti_le': 'Tỉ lệ'
}).fillna(0)

numeric_columns = ['NAV', 'Lãi lỗ sau cùng', 'Dư nợ hiện tại', 'Giá trị danh mục', 'Tỉ lệ']
for col in numeric_columns:
    nav_daily[col] = pd.to_numeric(nav_daily[col], errors='coerce').fillna(0)

# 👉 Lọc: chỉ hiển thị các KH có |NAV| >= 100,000
THRESH = 100_000
nav_daily_view = nav_daily[nav_daily['NAV'].abs() >= THRESH].copy()

# tránh lỗi highlight khi bảng rỗng
max_values = {
    col: (nav_daily_view[col].max() if not nav_daily_view.empty else 0)
    for col in numeric_columns
}

gb1 = GridOptionsBuilder.from_dataframe(nav_daily_view)
gb1.configure_default_column(editable=False, filter=True, resizable=True,
                             headerClass='centered', cellStyle={'textAlign': 'center'})
gb1.configure_column('Khách hàng', pinned='left', width=170,
                     cellStyle={'textAlign':'center'}, headerClass='centered')

for col in numeric_columns:
    js_style = JsCode(js_highlight_max_tpl.format(max_val=max_values[col]))
    gb1.configure_column(col, cellRenderer=js_number_or_percent_right,
                         cellStyle=js_style, width=140, headerClass='centered')

st.header('📈 NAV ngày')
AgGrid(
    nav_daily_view,
    gridOptions=gb1.build(),
    custom_css=custom_css,
    height=450,
    fit_columns_on_grid_load=True,
    theme='streamlit',
    allow_unsafe_jscode=True
)


st.markdown("<br>", unsafe_allow_html=True)

# ===========================
# 2. SỐ LƯỢNG MUA (Tổng ở cuối theo CỘT) + HEATMAP
# ===========================
query2 = '''
select khach_hang, ma, so_luong_mua
from key_in
where length(ma) = 3
  and on_off = 'ON'
  and khach_hang is not null
  and so_luong_mua != 0
'''
checkend_day = conn.execute(query2).fetchdf()

pivot = pd.pivot_table(
    checkend_day,
    values='so_luong_mua',
    index='khach_hang',
    columns='ma',
    aggfunc='sum',
    fill_value=0
)

# Thêm dòng Tổng theo CỘT ở CUỐI
pivot.loc['Tổng'] = pivot.sum(axis=0)

# Đặt tên index trước khi reset_index để KHÔNG sinh cột 'index'
pivot.index.name = 'Khách hàng'
pivot_ag = pivot.reset_index()

# Phòng ngừa nếu có cột 'index'
if 'index' in pivot_ag.columns and 'Khách hàng' in pivot_ag.columns:
    pivot_ag = pivot_ag.drop(columns=['index'])

# ==== TÍNH MAX MỖI CỘT (bỏ hàng 'Tổng') để scale màu ====
value_cols = [c for c in pivot_ag.columns if c != 'Khách hàng']
tmp = pivot_ag[pivot_ag['Khách hàng'] != 'Tổng']
col_max = {}
for c in value_cols:
    m = pd.to_numeric(tmp[c], errors='coerce').max()
    col_max[c] = 1 if pd.isna(m) or m <= 0 else float(m)

st.header('🛒 Số lượng mua')

gb2 = GridOptionsBuilder.from_dataframe(pivot_ag)
gb2.configure_default_column(resizable=True, headerClass='centered')
gb2.configure_column('Khách hàng', pinned='left', min_width=180,
                     cellStyle={'textAlign':'center'}, headerClass='centered')

# ==== cấu hình từng cột số: render số (căn phải) + heatmap nền xanh lá ====
for c in value_cols:
    max_val = col_max[c]
    heat_js = JsCode(f"""
        function(params) {{
            // Không tô màu cho dòng Tổng
            if (params.data && params.data['Khách hàng'] === 'Tổng') {{
                return {{textAlign:'right', fontWeight:'600', backgroundColor:'#f2f2f2'}};
            }}
            var raw = params.value;
            if (raw === null || raw === undefined || raw === '' || raw === 0) {{
                return {{textAlign:'right'}};
            }}
            var v = Number(String(raw).replace(/,/g,''));
            if (isNaN(v) || v <= 0) return {{textAlign:'right'}};

            // Chuẩn hoá 0..1 theo MAX của cột
            var r = Math.min(1, v/{max_val});

            // 🎨 Xanh lá: H=140°, S=75%, Lightness 96% (nhạt) -> 35% (đậm)
            var light = 96 - 61*r;                      // 96 → 35
            var bg = 'hsl(140, 75%,' + light.toFixed(1) + '%)';

            // Chữ trắng khi nền đậm, chữ đen khi nền nhạt
            var fg = (light < 55) ? 'white' : 'black';

            return {{ backgroundColor: bg, color: fg, textAlign:'right' }};
        }}
    """)
    gb2.configure_column(
        c,
        cellRenderer=js_number_right,   # format nghìn + căn phải
        cellStyle=heat_js,
        min_width=90,
        headerClass='centered'
    )

AgGrid(
    pivot_ag,
    gridOptions=gb2.build(),
    custom_css=custom_css,
    height=560,
    fit_columns_on_grid_load=True,
    theme='streamlit',
    allow_unsafe_jscode=True
)

st.markdown("<br>", unsafe_allow_html=True)

# ===========================
# 3.LÃI VAY THEO NGÀY

# Tạo bảng lãi vay theo ngày
query3 = '''
select khach_hang,
    ngay,
    lai_vay_ngay
from NAV_batch
'''
lai_ngay = conn.execute(query3).fetchdf()

# Tạo pivot table với tổng (margins=True)
pivot_2 = pd.pivot_table(
    NAV_batch,
    values='lai_vay_ngay',
    index='khach_hang',
    columns='ngay',
    aggfunc='sum',
    fill_value=None,
    # margins=True,
    # margins_name='Tong'
)
# Sắp xếp lại cột theo thời gian tăng dần
pivot_2 = pivot_2.sort_index(axis=1)

# Sắp xếp theo tổng hàng
pivot_2['__tong_tam__'] = pivot_2.sum(axis=1)
pivot_2 = pivot_2.sort_values(by='__tong_tam__', ascending=False).drop(columns='__tong_tam__')

# Thêm dòng tổng
tong_hang = pd.DataFrame(pivot_2.sum(axis=0)).T
tong_hang.index = ['Tổng']
pivot_2 = pd.concat([pivot_2, tong_hang])

# Chuyển cột về datetime nếu chưa
pivot_2.columns = pd.to_datetime(pivot_2.columns)

# Tính thay đổi tuyệt đối
pivot_2_no_total = pivot_2.drop(index='Tổng')
pivot_2_diff = pivot_2_no_total.diff(axis=1)
pivot_2_diff = pd.concat([pivot_2_diff, pd.DataFrame(index=['Tổng'], columns=pivot_2_diff.columns)])

# Tạo cột xen kẽ: giá trị + thay đổi tuyệt đối
merged_cols = []
for col in pivot_2.columns:
    col_str = col.strftime('%d/%m/%Y')
    merged_cols.append(col_str)
    if col != pivot_2.columns[0]:
        merged_cols.append(f'{col_str} (thay đổi)')

# Tạo DataFrame kết hợp
pivot_2_combined = pd.DataFrame(index=pivot_2.index, columns=merged_cols)

for col in pivot_2.columns:
    col_str = col.strftime('%d/%m/%Y')
    pivot_2_combined[col_str] = pivot_2[col]
    if col != pivot_2.columns[0]:
        diff_series = pivot_2_diff[col].apply(lambda x: f"+{x:,.0f}" if x > 0 else (f"{x:,.0f}" if x < 0 else ""))
        pivot_2_combined[f'{col_str} (thay đổi)'] = diff_series

# Đảo ngược thứ tự ngày
sorted_dates = sorted(pivot_2.columns, reverse=True)
final_col_order = []
for col in sorted_dates:
    col_str = col.strftime('%d/%m/%Y')
    final_col_order.append(col_str)
    diff_col = f'{col_str} (thay đổi)'
    if diff_col in pivot_2_combined.columns:
        final_col_order.append(diff_col)

pivot_2_combined = pivot_2_combined[final_col_order]

st.header('💰 Lãi vay theo ngày')

pivot_2_combined = pivot_2_combined.copy()
pivot_2_combined['khach_hang'] = pivot_2_combined.index
pivot_2_combined = pivot_2_combined.reset_index(drop=True)

# Tạo GridOptionsBuilder
gb = GridOptionsBuilder.from_dataframe(pivot_2_combined)

# 👇 Căn trái + tự động cao dòng nếu wrapText
gb.configure_default_column(
    cellStyle={'textAlign': 'left', 'whiteSpace': 'normal'},
    resizable=True,
    wrapText=True,
    autoHeight=True,
)

# ✅ Định nghĩa các JS để ẩn số 0 và highlight màu
js_zero_to_empty = JsCode("""
    function(params) {
        if (params.value === 0 || params.value === null || params.value === undefined) {
            return '';
        }
        return params.value.toLocaleString();
    }
""")

js_highlight = JsCode("""
    function(params) {
        if (params.value == null || params.value === '') return {};
        let v = params.value;
        if (typeof v === 'string') {
            v = parseFloat(v.replace(/,/g, '').replace('+', ''));
        }
        if (v > 0) return { color: 'green' };
        else if (v < 0) return { color: 'red' };
        return {};
    }
""")

# ✅ Cấu hình từng cột
for col in pivot_2_combined.columns:
    if col == 'khach_hang':
        gb.configure_column(col, pinned='left', min_width=180)
    elif '(thay đổi)' in col:
        gb.configure_column(col, cellRenderer=js_zero_to_empty, cellStyle=js_highlight, min_width=120)
    else:
        gb.configure_column(col, cellRenderer=js_zero_to_empty, min_width=90)

# Build grid config
gridOptions = gb.build()

row_height = 31
num_rows = len(pivot_2_combined)
table_height = row_height * num_rows -40


# ✅ Hiển thị AgGrid
AgGrid(
    pivot_2_combined,
    gridOptions=gridOptions,
    height=table_height,
    fit_columns_on_grid_load=False,  # Không auto-fit toàn bảng để giữ min_width
    allow_unsafe_jscode=True
) 

st.markdown("<br>", unsafe_allow_html=True)

# ===========================
# 4. TỔNG LÃI VAY THEO NGÀY (LINE)
# ===========================
st.header("📊 Tổng lãi vay theo ngày")

lai_tong = conn.execute("""
select ngay, sum(lai_vay_ngay) as lai_vay_tong
from NAV_batch
group by ngay
order by ngay
""").fetchdf().set_index('ngay')

st.line_chart(lai_tong['lai_vay_tong'])
