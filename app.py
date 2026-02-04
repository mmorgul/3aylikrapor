import streamlit as st
import datetime
import time
from epias_rapor_v3 import BakanlikCeyreklikVeri

# Sayfa Ayarları
st.set_page_config(
    page_title="Çeyreklik Veri Raporu",
    page_icon="⚡",
    layout="wide"
)

# Başlık
st.title("⚡ Çeyreklik Veri Raporu")
st.markdown("---")

# Sidebar - Giriş Bilgileri
with st.sidebar:
    st.header("🔐 Giriş Bilgileri")
    username = st.text_input("Kullanıcı Adı")
    password = st.text_input("Şifre", type="password")
    
    st.markdown("---")
    st.header("📅 Rapor Dönemi")
    
    current_year = datetime.datetime.now().year
    year = st.number_input("Yıl", min_value=2015, max_value=current_year + 1, value=current_year)
    quarter = st.selectbox("Çeyrek", [1, 2, 3, 4], index=3)  # Varsayılan Q4
    
    st.markdown("---")
    st.info("Bu uygulama EPİAŞ Şeffaflık Platformu'ndan veri çekerek Excel raporu oluşturur.")

# Ana Ekran
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("Rapor Oluşturma")
    st.write(f"Seçilen Dönem: **{year} - Q{quarter}**")
    
    if st.button("🚀 Raporu Oluştur", type="primary"):
        if not username or not password:
            st.error("Lütfen kullanıcı adı ve şifre giriniz!")
        else:
            # Log container
            log_container = st.empty()
            
            # Log fonksiyonu
            def log_message(msg):
                with log_container.container():
                    st.text(msg)
                    # Otomatik kaydırma için (Streamlit'te tam olmasa da) en son mesajı gösterir
            
            try:
                with st.spinner('Veriler çekiliyor ve işleniyor... Lütfen bekleyiniz.'):
                    # İlerleme çubuğu
                    progress_bar = st.progress(0)
                    
                    # İşlemi başlat
                    bcv = BakanlikCeyreklikVeri(username, password, (quarter, year), logger=st.write)
                    progress_bar.progress(20)
                    
                    bcv.download_data()
                    progress_bar.progress(70)
                    
                    bcv.format_data()
                    progress_bar.progress(90)
                    
                    excel_data = bcv.get_excel_bytes()
                    progress_bar.progress(100)
                
                st.success("✅ İşlem Başarıyla Tamamlandı!")
                
                # İndirme Butonu
                file_name = f"{year}-Q{quarter}-Data.xlsx"
                st.download_button(
                    label="📥 Excel Dosyasını İndir",
                    data=excel_data,
                    file_name=file_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
                
            except Exception as e:
                st.error(f"❌ Bir hata oluştu: {e}")
                with st.expander("Hata Detayı"):
                    st.write(str(e))

with col2:
    st.subheader("Bilgi")
    st.markdown("""
    **İşlem Adımları:**
    1. Kimlik doğrulama (TGT Token alma)
    2. API'den verilerin çekilmesi (PTF, SMF, GÖP, GİP vb.)
    3. Verilerin işlenmesi ve özet tablonun oluşturulması
    4. Excel dosyasının hazırlanması
    
    **Not:** İşlem verilerin yoğunluğuna göre 1-2 dakika sürebilir.
    """)
