# -*- coding: utf-8 -*-
"""
EPIAS Şeffaflık Platformu - Bakanlık Çeyreklik Veri Raporu V3
Modüler ve Dashboard uyumlu versiyon.
"""

import requests
import pandas as pd
import numpy as np
import datetime
import time
import io
from openpyxl import Workbook
from openpyxl.utils.dataframe import dataframe_to_rows

# ==================== SABİTLER ====================
# Rate limiting: 60 saniyede maksimum 50 istek
REQUEST_DELAY = 1.5  # saniye

# API Base URLs
AUTH_URL = "https://giris.epias.com.tr/cas/v1/tickets"
BASE_URL = "https://seffaflik.epias.com.tr/electricity-service"

# ==================== YARDIMCI FONKSİYONLAR ====================

def get_tgt_token(username: str, password: str) -> str:
    """EPIAS'tan TGT (Ticket Granting Ticket) token alır."""
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "text/plain"
    }
    payload = f"username={username}&password={password}"
    
    response = requests.post(AUTH_URL, data=payload, headers=headers)
    
    if response.status_code == 201:
        print("✓ TGT token başarıyla alındı.")
        return response.text.strip()
    else:
        raise Exception(f"Giriş Başarısız! Status: {response.status_code}, Mesaj: {response.text}")


def get_quarter():
    """Bir önceki çeyreği döndürür."""
    now = datetime.datetime.now()
    current_quarter = int(np.ceil(now.month / 3.0))
    current_year = now.year
    
    if current_quarter == 1:
        previous_quarter = (4, current_year - 1)
    else:
        previous_quarter = (current_quarter - 1, current_year)
    
    return previous_quarter


def quarter_to_dates(quarter_info: tuple) -> tuple:
    """Çeyrek bilgisini başlangıç ve bitiş tarihlerine çevirir.
    NOT: Rapor kümülatif olmalı (Yıl başından çeyrek sonuna kadar).
    """
    q, year = quarter_info
    
    # Başlangıç her zaman yılın başı
    start = f"{year}-01-01T00:00:00+03:00"
    
    # EPIAS API için tarih formatı: 2023-01-01T00:00:00+03:00
    if q == 1:
        end = f"{year}-03-31T23:00:00+03:00"
    elif q == 2:
        end = f"{year}-06-30T23:00:00+03:00"
    elif q == 3:
        end = f"{year}-09-30T23:00:00+03:00"
    elif q == 4:
        end = f"{year}-12-31T23:00:00+03:00"
    else:
        raise ValueError("Geçersiz çeyrek!")
    
    return (start, end)


def make_api_request(tgt: str, endpoint: str, payload: dict) -> dict:
    """EPIAS API'sine istek atar."""
    url = f"{BASE_URL}{endpoint}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "TGT": tgt
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers, timeout=60)
        
        # Rate limiting için bekle
        time.sleep(REQUEST_DELAY)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"  ! Hata: {endpoint.split('/')[-1]}, Status: {response.status_code}")
            return {"items": [], "body": {"items": []}}
    except requests.exceptions.Timeout:
        print(f"  ! Timeout: {endpoint.split('/')[-1]}")
        return {"items": [], "body": {"items": []}}
    except Exception as e:
        print(f"  ! İstek hatası: {e}")
        return {"items": [], "body": {"items": []}}


def fetch_paginated_data(tgt: str, endpoint: str, start_date: str, end_date: str, 
                         extra_params: dict = None, items_key: str = "items") -> list:
    """API'den veri çeker (basit versiyon, sayfalama yok)."""
    
    # Basit payload - sayfalama olmadan
    payload = {
        "startDate": start_date,
        "endDate": end_date
    }
    
    if extra_params:
        payload.update(extra_params)
    
    result = make_api_request(tgt, endpoint, payload)
    
    # items farklı yerlerde olabilir
    items = result.get(items_key, [])
    if not items:
        items = result.get("body", {}).get(items_key, [])
    
    if items:
        print(f"  ✓ {len(items)} kayıt çekildi ({endpoint.split('/')[-1]})")
    
    return items


def items_to_dataframe(items: list, prefix: str = "") -> pd.DataFrame:
    """API sonuçlarını güvenli bir şekilde DataFrame'e çevirir."""
    if not items:
        return pd.DataFrame()
    
    df = pd.DataFrame(items)
    
    # date sütunu varsa index olarak kullan
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        if df["date"].dt.tz is not None:
            df["date"] = df["date"].dt.tz_localize(None)
        df = df.set_index("date")
    
    # Gereksiz hour sütununu kaldır
    if "hour" in df.columns:
        df = df.drop(columns=["hour"])
    
    # prefix ekle
    if prefix:
        df.columns = [f"{prefix}{c}" for c in df.columns]
    
    return df


# ==================== VERİ ÇEKME FONKSİYONLARI ====================

def fetch_ptf_smf(tgt: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Piyasa Takas Fiyatı (PTF) verilerini çeker."""
    # PTF
    ptf_items = fetch_paginated_data(tgt, "/v1/markets/dam/data/mcp", start_date, end_date)
    return items_to_dataframe(ptf_items, prefix="ptf_")


def fetch_smf(tgt: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Sistem Marjinal Fiyatı (SMF) verilerini çeker."""
    items = fetch_paginated_data(tgt, "/v1/markets/bpm/data/system-marginal-price", start_date, end_date)
    return items_to_dataframe(items, prefix="smf_")


def fetch_system_direction(tgt: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Sistem Yönü verilerini çeker."""
    items = fetch_paginated_data(tgt, "/v1/markets/bpm/data/system-direction", start_date, end_date)
    return items_to_dataframe(items, prefix="sysdir_")


def fetch_bilateral_contracts(tgt: str, start_date: str, end_date: str) -> pd.DataFrame:
    """İkili Anlaşma miktarlarını çeker."""
    items = fetch_paginated_data(tgt, "/v1/markets/bilateral-contracts/data/bilateral-contracts-bid-quantity", start_date, end_date)
    return items_to_dataframe(items, prefix="bilateral_")


def fetch_dam_clearing_quantity(tgt: str, start_date: str, end_date: str) -> pd.DataFrame:
    """GÖP Eşleşme Miktarı verilerini çeker."""
    items = fetch_paginated_data(tgt, "/v1/markets/dam/data/clearing-quantity", start_date, end_date)
    return items_to_dataframe(items, prefix="dam_")


def fetch_bpm_orders(tgt: str, start_date: str, end_date: str) -> tuple:
    """Yük Atma (YAT) ve Yük Alma (YAL) talimat miktarlarını çeker."""
    # Yük Atma (DOWN)
    down_items = fetch_paginated_data(tgt, "/v1/markets/bpm/data/order-summary-down", start_date, end_date)
    # Yük Alma (UP)
    up_items = fetch_paginated_data(tgt, "/v1/markets/bpm/data/order-summary-up", start_date, end_date)
    
    df_down = items_to_dataframe(down_items, prefix="bpmD_")
    df_up = items_to_dataframe(up_items, prefix="bpmU_")
    
    return df_down, df_up


def fetch_idm_data(tgt: str, start_date: str, end_date: str) -> tuple:
    """GİP Ağırlıklı Ortalama Fiyat ve Eşleşme Miktarı verilerini çeker."""
    # Ağırlıklı Ortalama Fiyat
    price_items = fetch_paginated_data(tgt, "/v1/markets/idm/data/weighted-average-price", start_date, end_date)
    # Eşleşme Miktarı
    quantity_items = fetch_paginated_data(tgt, "/v1/markets/idm/data/matching-quantity", start_date, end_date)
    
    df_price = items_to_dataframe(price_items, prefix="idm_")
    
    # Matching Quantity için özel işlem (Tarih verisi kontrat adından çekilecek)
    if quantity_items:
        df_quant = pd.DataFrame(quantity_items)
        if "kontratAdi" in df_quant.columns:
            # Kontrat adı formatı: PH23010110 (YYMMDDHH) -> sondaki saati de alıyoruz
            try:
                # PH (2 karakter) atılıyor -> YYMMDDHH
                df_quant["date"] = pd.to_datetime(df_quant["kontratAdi"].str[2:], format='%y%m%d%H', errors='coerce')
                # Hatalı dönüşümleri temizle
                df_quant = df_quant.dropna(subset=["date"])
                df_quant = df_quant.set_index("date")
            except Exception as e:
                print(f"  ! GİP Tarih ayrıştırma hatası: {e}")
        
        df_quant.columns = ["idm_" + c for c in df_quant.columns]
    else:
        df_quant = pd.DataFrame()
    
    return df_price, df_quant


def fetch_ancillary_services(tgt: str, start_date: str, end_date: str) -> dict:
    """Primer ve Sekonder Frekans Kapasite ve Fiyat verilerini çeker."""
    results = {}
    
    # Primer Frekans Kapasite Miktarı
    pfc_items = fetch_paginated_data(tgt, "/v1/markets/ancillary-services/data/primary-frequency-capacity-amount", start_date, end_date)
    results["pfc_amount"] = items_to_dataframe(pfc_items)
    
    # Primer Frekans Kapasite Fiyatı
    pfp_items = fetch_paginated_data(tgt, "/v1/markets/ancillary-services/data/primary-frequency-capacity-price", start_date, end_date)
    results["pfp_price"] = items_to_dataframe(pfp_items)
    
    # Sekonder Frekans Kapasite Miktarı
    sfc_items = fetch_paginated_data(tgt, "/v1/markets/ancillary-services/data/secondary-frequency-capacity-amount", start_date, end_date)
    results["sfc_amount"] = items_to_dataframe(sfc_items)
    
    # Sekonder Frekans Kapasite Fiyatı
    sfp_items = fetch_paginated_data(tgt, "/v1/markets/ancillary-services/data/secondary-frequency-capacity-price", start_date, end_date)
    results["sfp_price"] = items_to_dataframe(sfp_items)
    
    return results


# ==================== ANA SINIF ====================

class BakanlikCeyreklikVeri:
    """Bakanlık Çeyreklik Veri Raporu oluşturucu."""
    
    def __init__(self, username, password, quarter_info: tuple = None, logger=print):
        """
        Args:
            username: EPIAS kullanıcı adı
            password: EPIAS şifre
            quarter_info: (çeyrek, yıl) formatında tuple. Örn: (4, 2024)
            logger: Loglama fonksiyonu (örn: st.write veya print)
        """
        self.username = username
        self.password = password
        self.log = logger
        
        self.log("=" * 50)
        self.log("Çeyreklik Veri Raporu Başlatılıyor...")
        self.log("=" * 50)
        
        # TGT Token al
        self.log("🔑 Giriş yapılıyor...")
        self.tgt = get_tgt_token(self.username, self.password)
        
        # Çeyrek bilgisini belirle
        if quarter_info is None:
            self.quarter_info = get_quarter()
        else:
            if quarter_info[0] not in [1, 2, 3, 4] or quarter_info[1] < 2015:
                raise ValueError("Tarihleri kontrol ediniz. Çeyrek 1-4, yıl >= 2015 olmalı.")
            self.quarter_info = quarter_info
        
        self.start_date, self.end_date = quarter_to_dates(self.quarter_info)
        self.log(f"📅 Dönem: {self.quarter_info[1]} Q{self.quarter_info[0]} ({self.start_date[:10]} - {self.end_date[:10]})")
        
        self.df = None
        self.ozet = None
        self.final_result = None
    
    def download_data(self):
        """Tüm verileri API'den çeker."""
        self.log("\n📥 Veriler çekiliyor...")
        
        # PTF/SMF
        self.log("- PTF ve SMF...")
        df_ptf = fetch_ptf_smf(self.tgt, self.start_date, self.end_date)
        df_smf = fetch_smf(self.tgt, self.start_date, self.end_date)
        
        # Sistem Yönü
        self.log("- Sistem Yönü...")
        df_sysdir = fetch_system_direction(self.tgt, self.start_date, self.end_date)
        
        # İkili Anlaşmalar
        self.log("- İkili Anlaşmalar...")
        df_bilateral = fetch_bilateral_contracts(self.tgt, self.start_date, self.end_date)
        
        # GÖP Eşleşme Miktarı
        self.log("- GÖP Eşleşme Miktarı...")
        df_dam = fetch_dam_clearing_quantity(self.tgt, self.start_date, self.end_date)
        
        # BPM (YAL/YAT)
        self.log("- Dengeleme Güç Piyasası (YAL/YAT)...")
        df_bpm_down, df_bpm_up = fetch_bpm_orders(self.tgt, self.start_date, self.end_date)
        
        # GİP
        self.log("- Gün İçi Piyasası...")
        df_idm_price, df_idm_quant = fetch_idm_data(self.tgt, self.start_date, self.end_date)
        
        # Yan Hizmetler
        self.log("- Yan Hizmetler...")
        ancillary = fetch_ancillary_services(self.tgt, self.start_date, self.end_date)
        
        # Tüm verileri birleştir
        all_dfs = [df_ptf, df_smf, df_sysdir, df_bilateral, df_dam, df_bpm_down, df_bpm_up, 
                   df_idm_price, df_idm_quant]
        
        for key, df in ancillary.items():
            df.columns = [f"{key}_{c}" for c in df.columns]
            all_dfs.append(df)
        
        # Boş olmayan DataFrame'leri birleştir
        valid_dfs = [df for df in all_dfs if not df.empty]
        
        if valid_dfs:
            self.df = pd.concat(valid_dfs, axis=1)
            # Duplicate index'leri temizle
            self.df = self.df[~self.df.index.duplicated(keep='first')]
            self.log(f"✓ Toplam {len(self.df)} satır veri başarılı bir şekilde birleştirildi.")
        else:
            self.log("⚠ Uyarı: Hiç veri çekilemedi. Boş bir rapor oluşturulacak.")
            self.df = pd.DataFrame()
    
    def format_data(self):
        """Verileri formatlar ve özet oluşturur."""
        self.log("\n📊 Veriler analiz ediliyor...")
        
        fresult = {}
        
        # İkili Anlaşma Miktarı (milyar kWh)
        if "bilateral_quantity" in self.df.columns:
            fresult["bilateral_quantity"] = self.df["bilateral_quantity"].sum() / 1e6
        else:
            fresult["bilateral_quantity"] = 0
        
        # GÖP Eşleşme Miktarı (milyar kWh)
        dam_col = [c for c in self.df.columns if "dam_" in c.lower() and "matched" in c.lower()]
        if dam_col:
            fresult["dam_matchedBids"] = self.df[dam_col[0]].sum() / 1e6
        else:
            fresult["dam_matchedBids"] = 0
        
        # Ortalama PTF
        ptf_col = [c for c in self.df.columns if "ptf_" in c and ("price" in c.lower() or "mcp" in c.lower())]
        if ptf_col:
            fresult["ptf"] = self.df[ptf_col[0]].mean()
        else:
            fresult["ptf"] = 0
        
        # Ortalama SMF
        smf_col = [c for c in self.df.columns if "smf_" in c and ("price" in c.lower() or "smp" in c.lower() or "systemMarginalPrice" in c)]
        if smf_col:
            fresult["smf"] = self.df[smf_col[0]].mean()
        else:
            fresult["smf"] = 0
        
        # GİP Ağırlıklı Ortalama Fiyat
        wap_col = [c for c in self.df.columns if "idm_" in c and "wap" in c.lower()]
        if wap_col:
            fresult["idm_wap"] = self.df[wap_col[0]].mean()
        else:
            fresult["idm_wap"] = 0
        
        # GİP Eşleşme Miktarı
        idm_quant_col = [c for c in self.df.columns if "idm_" in c and ("quantity" in c.lower() or "clearing" in c.lower())]
        if idm_quant_col:
            # Sütun adı idm_clearingQuantityAsk veya idm_eslesmeMiktari olabilir
            quant_col_name = idm_quant_col[0]
            fresult["idm_quant"] = self.df[quant_col_name].sum() / 1e6
            
            # Yıllık Ağırlıklı Ortalama Fiyat (Quantity * WAP).sum() / Quantity.sum()
            # WAP sütununu bul
            if wap_col:
                wap_col_name = wap_col[0]
                try:
                    # Hesaplama: sum(Miktar * Fiyat) / sum(Miktar)
                    total_vol = self.df[quant_col_name].sum()
                    if total_vol > 0:
                        weighted_sum = (self.df[quant_col_name] * self.df[wap_col_name]).sum()
                        fresult["idm_year_price"] = weighted_sum / total_vol
                    else:
                        fresult["idm_year_price"] = 0
                except Exception as e:
                    self.log(f"⚠ GİP Ağırlıklı Ortalama hesaplanamadı: {e}")
                    fresult["idm_year_price"] = 0
            else:
                fresult["idm_year_price"] = 0
        else:
            fresult["idm_quant"] = 0
            fresult["idm_year_price"] = 0
        
        # BPM Talimatları
        # 0, 1, 2 Kodlu
        for code in ["ZeroCoded", "OneCoded", "TwoCoded"]:
            down_col = [c for c in self.df.columns if "bpmD_" in c and code.lower() in c.lower()]
            up_col = [c for c in self.df.columns if "bpmU_" in c and code.lower() in c.lower()]
            
            total = 0
            if down_col:
                total += self.df[down_col[0]].abs().sum()
            if up_col:
                total += self.df[up_col[0]].abs().sum()
            
            # Anahtarları reference koda uygun isimlendir: zero_coded, one_coded...
            key_name = ""
            if code == "ZeroCoded": key_name = "zero_coded"
            elif code == "OneCoded": key_name = "one_coded"
            elif code == "TwoCoded": key_name = "two_coded"
            
            fresult[key_name] = total / 1e6
        
        # Kesinleşmiş talimatlar
        down_delivered_col = [c for c in self.df.columns if "bpmD_" in c and "delivered" in c.lower()]
        up_delivered_col = [c for c in self.df.columns if "bpmU_" in c and "delivered" in c.lower()]
        
        fresult["down_delivered"] = self.df[down_delivered_col[0]].abs().sum() / 1e6 if down_delivered_col else 0
        fresult["up_delivered"] = self.df[up_delivered_col[0]].abs().sum() / 1e6 if up_delivered_col else 0
        
        # Frekans Kapasiteleri
        for key in ["pfc_amount", "pfp_price", "sfc_amount", "sfp_price"]:
            cols = [c for c in self.df.columns if key in c.lower()]
            if cols:
                fresult[key] = self.df[cols[0]].mean()
            else:
                fresult[key] = 0
        
        # Özet DataFrame oluştur
        ozet_data = {
            "Gösterge": [
                "Alış veya Satış Miktarı (milyar kWh)",
                "Ortalama Piyasa Takas Fiyatı (TL/MWh) (SST/SSM)",
                "Eşleşen Alış veya Satış Miktarı (milyar kWh)",
                "Günlük Ağırlıklı Ortalama Fiyatların, Yıl Bazında Aritmetik Ortalama Fiyatı (TL/MWh)",
                "Yıllık Ağırlıklı Ortalama Fiyat (TL/kWh) (SST/SSM)",  # Yeni eklenen satır
                "Eşleşme Miktarı (milyar kWh)",
                "Ortalama Sistem Marjinal Fiyatı (TL/MWh)",
                "0 Kodlu YAL ve YAT Talimatları Toplamı (milyar kWh)",
                "1 Kodlu YAL ve YAT Talimatları Toplamı (milyar kWh)",
                "2 Kodlu YAL ve YAT Talimatları Toplamı (milyar kWh)",
                "Kesinleşmiş Yük Alma Miktarı (milyar kWh)",
                "Kesinleşmiş Yük Atma Miktarı (milyar kWh)",
                "Ortalama Saatlik Primer Frekans Rezerv Miktarı (MWh)",
                "Ortalama Primer Frekans Kontrolü Fiyatı (TL/MWh)",
                "Ortalama Saatlik Sekonder Frekans Rezerv Miktarı (MWh)",
                "Ortalama Sekonder Frekans Kontrolü Fiyatı (TL/MWh)",
            ],
            "Değer": [
                fresult.get("bilateral_quantity", 0),
                fresult.get("ptf", 0),
                fresult.get("dam_matchedBids", 0),
                fresult.get("idm_wap", 0),
                fresult.get("idm_year_price", 0),  # Yeni eklenen değer
                fresult.get("idm_quant", 0),
                fresult.get("smf", 0),
                fresult.get("zero_coded", 0),
                fresult.get("one_coded", 0),
                fresult.get("two_coded", 0),
                fresult.get("up_delivered", 0),
                fresult.get("down_delivered", 0),
                fresult.get("pfc_amount", 0),
                fresult.get("pfp_price", 0),
                fresult.get("sfc_amount", 0),
                fresult.get("sfp_price", 0),
            ]
        }
        
        self.ozet = pd.DataFrame(ozet_data)
        self.final_result = fresult
        self.log("✓ Özet tablo oluşturuldu.")
    
    def get_excel_bytes(self) -> io.BytesIO:
        """Verileri Excel dosyası olarak (bytes) döndürür."""
        self.log("\n💾 Excel dosyası hazırlanıyor...")
        
        output = io.BytesIO()
        
        try:
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # Özet sayfası
                if self.ozet is not None:
                    self.ozet.to_excel(writer, sheet_name="Özet", index=False)
                    # Sütun genişlikleri (basic)
                    ws = writer.sheets["Özet"]
                    ws.column_dimensions['A'].width = 70
                    ws.column_dimensions['B'].width = 25
                
                # Detay sayfası
                if self.df is not None and not self.df.empty:
                    # Timezone temizliği yap
                    df_export = self.df.copy().reset_index()
                    
                    # Sütun İsimlerini Düzelt (System Status ve Yan Hizmetler)
                    rename_map = {
                        "sysdir_direction": "Sistem Yönü",
                        "pfc_amount_amount": "pfc_amount",
                        "pfp_price_price": "pfp_price",
                        "sfc_amount_amount": "sfc_amount",
                        "sfp_price_price": "sfp_price"
                    }
                    df_export = df_export.rename(columns=rename_map)
                    
                    # Sütunlarda timezone varsa temizle
                    for col in df_export.columns:
                        # Datetime sütunları - String'e çevir (Nuclear Option)
                        if pd.api.types.is_datetime64_any_dtype(df_export[col]):
                            df_export[col] = df_export[col].dt.strftime('%Y-%m-%d %H:%M:%S').fillna('')
                        
                        # Object sütunları - İçindeki Timestamp'leri string'e çevir
                        elif df_export[col].dtype == 'object':
                            df_export[col] = df_export[col].apply(lambda x: str(x) if isinstance(x, (datetime.date, datetime.datetime, pd.Timestamp)) else x)
                    
                    df_export.to_excel(writer, sheet_name="Detay", index=False)
                else:
                    pd.DataFrame({"Durum": ["Veri bulunamadı"]}).to_excel(writer, sheet_name="Detay", index=False)
            
            output.seek(0)
            self.log("✓ Excel dosyası bellekte oluşturuldu.")
            return output
            
        except Exception as e:
            self.log(f"❌ Excel oluşturma hatası: {e}")
            raise
