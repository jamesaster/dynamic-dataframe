import pandas as pd
import hashlib
import base64

def insert_hash_sku_imei(df: pd.DataFrame, cust_salt, anonym_path='../data_output/Anonym_Price.csv') -> pd.DataFrame:
   """
   ## Stage 5 - Applying hashed SKU, product fingerprints hashing 
   Thực hiện ánh xạ mã SKU ẩn danh và tạo dấu vân tay (fingerprint) bảo mật cho IMEI/Serial.

   Quy trình thực hiện:
   1. Chuẩn hóa khóa truy xuất (Lookup Key): Định dạng lại cột 'ean' (loại bỏ đuôi '.0', 
      strip khoảng trắng) để đảm bảo khớp dữ liệu chính xác giữa các bảng.
   2. Ánh xạ SKU (SKU Mapping): Sử dụng từ điển tra cứu từ dữ liệu 'Anonym_Price' 
      để chèn cột 'sku' vào vị trí cố định trong DataFrame.
   3. Băm IMEI (IMEI Hashing): Sử dụng thuật toán SHA-256 kết hợp với 'cust_salt' 
      để tạo mã băm cho IMEI/Serial. 
   4. Rút gọn mã băm: Mã hóa kết quả SHA-256 sang Base32 và thực hiện kỹ thuật 
      slicing (lấy mỗi 5 ký tự) để tạo dấu vân tay ngắn gọn (50 ký tự đầu).
   5. Xử lý dữ liệu khuyết: Gán nhãn 'unknown' cho các giá trị IMEI không xác định 
      hoặc không có trong danh sách băm.

   """
   # NOTE: Generate cryptographic fingerprints for product IMEI/Serials via SHA-256.

   # Result from step 5.
   if isinstance(anonym_path, pd.DataFrame):
      anonym_price = anonym_path
   else:
      anonym_price = pd.read_csv(anonym_path)
   # Normalize dtype and format in order to mapping
   def normalize4_lookup(series:pd.Series)-> pd.Series:
      """
      Convert dtype & format of lookup key.\n
      Remove '.0' at the end of EAN string.\n
      Strip.
      """
      return series.astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
   anonym_price['ean'] = anonym_price['ean'].pipe(normalize4_lookup)
   df['ean']           = df['ean'].pipe(normalize4_lookup)

   # Create lookup_Series with index=key_word
   lookup_dict = anonym_price.set_index('ean')['master_sku']
   # Create 'sku' column from Anonym_Price
   df.insert(4, 'sku', df['ean'].map(lookup_dict))

   def imei_hashing(series: pd.Series, key) -> pd.Series:
      unique_imeis = series[series!='unknown'].dropna().unique()
      
      salt = str(key).encode()
      hash_map = {}
      for imei in unique_imeis:
         h = hashlib.sha256(str(imei).encode() + salt).digest()
         hash_map[imei] = base64.b32encode(h).decode()[0:50:5]
   
      return series.map(hash_map).fillna('unknown')
   
   df['imei_sn'] = imei_hashing(df['imei_sn'], cust_salt)

   return df