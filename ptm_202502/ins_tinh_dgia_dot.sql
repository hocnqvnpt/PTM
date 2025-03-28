select THANG_PTM, MA_GD, MA_TB, DICH_VU, DTHU_GOI, LYDO_KHONGTINH_DONGIA, manv_hotro from ttkd_bsc.ct_bsc_ptm where ma_tb = 'hcm_edu_00009998';'hcm_connect_00000081';

select * from ttkd_bsc.ct_bsc_ptm where ma_tb = 'hcm_connect_00000081';
select * from ttkd_bsc.ct_bsc_ptm where ma_gd = 'HCM-LD/02044942';

select * from ttkd_bsc.ct_bsc_ptm where thang_luong = 86;

update ttkd_bsc.ct_bsc_ptm set LYDO_KHONGTINH_DONGIA = null, KIEMTRA_DUAN = 1, HESO_QUYDINH_NVPTM =1
where ma_gd = 'HCM-LD/02007793';
commit;
rollback;

id in (9245837, 10429028);

update ttkd_bsc.ct_bsc_ptm set DTHU_GOI = (40000000/1.1)
		--ma_tb = 'hcm_vbn_00001077', dich_vu = 'Voice Brandname', thuebao_id = 12206006, loaitb_id = 358, nop_du = 1
--		LYDO_KHONGTINH_DONGIA = null
--		,ghi_chu = ghi_chu || '; dot 1; 4697/TTr-KHDN3-KTKH', NOCUOC_N2 = null
where  id = 10429028;

select * from ttkd_bsc.bangluong_kpi where thang = 202412;
insert into 
select * from ttkd_bsc.nhanvien where thang = 202411 and ma_to = 'VNP0700820' and ma_vtcv is null;

select * from ttkd_bsc.bangluong_dongia_202412 a ;

insert into ttkd_bsc.ct_bsc_ptm
(THANG_LUONG, THANG_PTM, MA_GD, MA_KH, MA_TB, SOHOPDONG, DICH_VU, TENKIEU_LD, TEN_TB, DIACHI_LD, SO_NHA, AP_ID, KHU_ID, PHO_ID, PHUONG_ID, QUAN_ID, TINH_ID
, SO_GT, MST, MST_TT, SDT_LH, EMAIL_LH, CHUQUAN_ID, ID_447, NGAY_BBBG, NGAY_CAT, NGAY_LUUHS_TTKD, NGAY_LUUHS_TTVT, NGAY_SCAN, SCANBOOK_DU, NOP_DU, MIEN_HSGOC
, BS_LUUKHO, THOIHAN_ID, TG_THUE_TU, TG_THUE_DEN, SONGAY_SD, MA_DA, CHU_NHOM, VNP_MOI, GOI_CUOC, NHANVIEN_NHAN_ID, PBH_NHAN_ID, PBH_NHAN_GOC_ID, DICHVUVT_ID
, LOAITB_ID, HDKH_ID, HDTB_ID, KIEUHD_ID, KIEULD_ID, LOAIHD_ID, KIEUTN_ID, KHACHHANG_ID, THANHTOAN_ID, THUEBAO_ID, THUEBAO_CHA_ID, DOITUONG_ID, DOITUONG_CT_ID, TOCDO_ID
, MUCUOCTB_ID, GOI_ID, PHANLOAI_ID, SL_MAILING, DOIVT_ID, TTVT_ID, MA_TIEPTHI, MA_TIEPTHI_NEW, TO_TT_ID, DONVI_TT_ID, DONVI_TT, NHANVIENGT_ID, MA_NGUOIGT, NGUOI_GT, NHOM_GT, MANV_TT_DAI
, MA_TO_DAI, MA_VTCV_DAI, MANV_HOTRO, TYLE_HOTRO, TYLE_AM, GHI_CHU, LYDO_KHONGTINH_LUONG, KH_ID, MA_DT_KH, PBH_DB_ID, PBH_QL_ID, PBH_PTM_ID, MA_PB, TEN_PB, MA_TO, TEN_TO, MANV_PTM, TENNV_PTM
, MA_VTCV, LOAINV_ID, TEN_VTCV, LOAI_LD, NHOM_TIEPTHI, DATCOC_CSD, THANG_BDDC, THANG_KTDC, SOTHANG_DC, CHIPHI_TTKD, THANG_XN_CHIPHI_TTKD, TYLE_HUONGDT, TIEN_DNHM, TIEN_SODEP, TIEN_CAMKET, NGAY_TT
, SOSERI, TIEN_TT, HT_TRA_ID, DTHU_PS_TRUOC, DTHU_PS, DTHU_PS_N1, DTHU_PS_N2, DTHU_PS_N3, TRANGTHAITB_ID, TRANGTHAITB_ID_N1, TRANGTHAITB_ID_N2, TRANGTHAITB_ID_N3, TRANGTHAITB_ID_N5, NOCUOC_PTM, NOCUOC_N1
, NOCUOC_N2, NOCUOC_N3, NOCUOC_T1, NOCUOC_T2, XACNHAN_KHKT, THANG_XACNHAN_KHKT, DOITUONG_KH, KHHH_KHM, DIABAN, DTHU_GOI_GOC, DTHU_GOI, DTHU_GOI_NGOAIMANG, HESO_DICHVU, HESO_DICHVU_1, PHANLOAI_KH
, HESO_KHACHHANG, HESO_TBNGANHAN, HESO_TRATRUOC, HESO_KHUYENKHICH, HESO_KVDACTHU, HESO_VTCV_NVPTM, HESO_VTCV_DAI, HESO_VTCV_NVHOTRO, HESO_HOTRO_NVPTM, HESO_HOTRO_DAI, HESO_HOTRO_NVHOTRO
, HESO_QUYDINH_NVPTM, HESO_QUYDINH_DAI, HESO_QUYDINH_NVHOTRO, HESO_DIABAN_TINHKHAC, HESO_HOSO, HESO_DICHVU_DNHM, DONGIA, DOANHTHU_DONGIA_NVPTM, DOANHTHU_KPI_NVPTM, LUONG_DONGIA_NVPTM, DOANHTHU_DONGIA_DAI
, DOANHTHU_KPI_NVDAI, LUONG_DONGIA_DAI, DOANHTHU_DONGIA_NVHOTRO, DOANHTHU_KPI_NVHOTRO, LUONG_DONGIA_NVHOTRO, DOANHTHU_KPI_TO, DOANHTHU_KPI_PHONG, DOANHTHU_KPI_DNHM, DOANHTHU_DONGIA_DNHM
, LUONG_DONGIA_DNHM_NVPTM, DOANHTHU_KPI_DNHM_PHONG, THANG_TLDG_DNHM, THANG_TLKPI_DNHM, THANG_TLKPI_DNHM_TO, THANG_TLKPI_DNHM_PHONG, THANG_TLDG_DT, THANG_TLKPI, THANG_TLDG_DT_NVHOTRO, THANG_TLKPI_HOTRO
, THANG_TLDG_DT_DAI, THANG_TLKPI_TO, THANG_TLKPI_PHONG, LYDO_KHONGTINH_DONGIA, NOCUOC_THUHOI, THANG_TLDG_TH, SOTHANGNO_THUHOI, LINE_ID, LINE_NVAM_QL, MA_DUAN_BANHANG, KIEMTRA_DUAN, DIABAN_ID, LOAI_TB, VANBAN_ID
, NGUON, TRANGTHAI_TT_ID, USER_CN, MANV_CN)
select 302 THANG_LUONG, THANG_PTM, MA_GD, MA_KH, MA_TB, SOHOPDONG, DICH_VU, TENKIEU_LD, TEN_TB, DIACHI_LD, SO_NHA, AP_ID, KHU_ID, PHO_ID, PHUONG_ID, QUAN_ID, TINH_ID
, SO_GT, MST, MST_TT, SDT_LH, EMAIL_LH, CHUQUAN_ID, ID_447, NGAY_BBBG, NGAY_CAT, NGAY_LUUHS_TTKD, NGAY_LUUHS_TTVT, NGAY_SCAN, SCANBOOK_DU, NOP_DU, MIEN_HSGOC
, BS_LUUKHO, THOIHAN_ID, TG_THUE_TU, TG_THUE_DEN, SONGAY_SD, MA_DA, CHU_NHOM, VNP_MOI, GOI_CUOC, NHANVIEN_NHAN_ID, PBH_NHAN_ID, PBH_NHAN_GOC_ID, DICHVUVT_ID
, LOAITB_ID, HDKH_ID, HDTB_ID, KIEUHD_ID, KIEULD_ID, LOAIHD_ID, KIEUTN_ID, KHACHHANG_ID, THANHTOAN_ID, THUEBAO_ID, THUEBAO_CHA_ID, DOITUONG_ID, DOITUONG_CT_ID, TOCDO_ID
, MUCUOCTB_ID, GOI_ID, PHANLOAI_ID, SL_MAILING, DOIVT_ID, TTVT_ID, MA_TIEPTHI, MA_TIEPTHI_NEW, TO_TT_ID, DONVI_TT_ID, DONVI_TT, NHANVIENGT_ID, MA_NGUOIGT, NGUOI_GT, NHOM_GT, MANV_TT_DAI
, MA_TO_DAI, MA_VTCV_DAI, MANV_HOTRO, TYLE_HOTRO, TYLE_AM, GHI_CHU, LYDO_KHONGTINH_LUONG, KH_ID, MA_DT_KH, PBH_DB_ID, PBH_QL_ID, PBH_PTM_ID, MA_PB, TEN_PB, MA_TO, TEN_TO, MANV_PTM, TENNV_PTM
, MA_VTCV, LOAINV_ID, TEN_VTCV, LOAI_LD, NHOM_TIEPTHI, DATCOC_CSD, THANG_BDDC, THANG_KTDC, SOTHANG_DC, CHIPHI_TTKD, THANG_XN_CHIPHI_TTKD, TYLE_HUONGDT, TIEN_DNHM, TIEN_SODEP, TIEN_CAMKET, NGAY_TT
, SOSERI, TIEN_TT, HT_TRA_ID, DTHU_PS_TRUOC, DTHU_PS, DTHU_PS_N1, DTHU_PS_N2, DTHU_PS_N3, TRANGTHAITB_ID, TRANGTHAITB_ID_N1, TRANGTHAITB_ID_N2, TRANGTHAITB_ID_N3, TRANGTHAITB_ID_N5, NOCUOC_PTM, NOCUOC_N1
, NOCUOC_N2, NOCUOC_N3, NOCUOC_T1, NOCUOC_T2, XACNHAN_KHKT, THANG_XACNHAN_KHKT, DOITUONG_KH, KHHH_KHM, DIABAN, DTHU_GOI_GOC, DTHU_GOI, DTHU_GOI_NGOAIMANG, HESO_DICHVU, HESO_DICHVU_1, PHANLOAI_KH
, HESO_KHACHHANG, HESO_TBNGANHAN, HESO_TRATRUOC, HESO_KHUYENKHICH, HESO_KVDACTHU, HESO_VTCV_NVPTM, HESO_VTCV_DAI, HESO_VTCV_NVHOTRO, HESO_HOTRO_NVPTM, HESO_HOTRO_DAI, HESO_HOTRO_NVHOTRO
, HESO_QUYDINH_NVPTM, HESO_QUYDINH_DAI, HESO_QUYDINH_NVHOTRO, HESO_DIABAN_TINHKHAC, HESO_HOSO, HESO_DICHVU_DNHM, DONGIA, DOANHTHU_DONGIA_NVPTM, DOANHTHU_KPI_NVPTM, LUONG_DONGIA_NVPTM, DOANHTHU_DONGIA_DAI
, DOANHTHU_KPI_NVDAI, LUONG_DONGIA_DAI, DOANHTHU_DONGIA_NVHOTRO, DOANHTHU_KPI_NVHOTRO, LUONG_DONGIA_NVHOTRO, DOANHTHU_KPI_TO, DOANHTHU_KPI_PHONG, DOANHTHU_KPI_DNHM, DOANHTHU_DONGIA_DNHM
, LUONG_DONGIA_DNHM_NVPTM, DOANHTHU_KPI_DNHM_PHONG, THANG_TLDG_DNHM, THANG_TLKPI_DNHM, THANG_TLKPI_DNHM_TO, THANG_TLKPI_DNHM_PHONG, THANG_TLDG_DT, THANG_TLKPI, THANG_TLDG_DT_NVHOTRO, THANG_TLKPI_HOTRO
, THANG_TLDG_DT_DAI, THANG_TLKPI_TO, THANG_TLKPI_PHONG, LYDO_KHONGTINH_DONGIA, NOCUOC_THUHOI, THANG_TLDG_TH, SOTHANGNO_THUHOI, LINE_ID, LINE_NVAM_QL, MA_DUAN_BANHANG, KIEMTRA_DUAN, DIABAN_ID, LOAI_TB, VANBAN_ID
, NGUON, TRANGTHAI_TT_ID, USER_CN, MANV_CN
from ttkd_bsc.ct_bsc_ptm
where thang_ptm >=202412 and vanban_id = 1113868;

rollback;
commit;

