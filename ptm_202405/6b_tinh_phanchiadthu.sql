Theo VB 977/VNPT.TPHCM-PTTT ngay 29/3/2022, quy dinh co che kinh te san pham dich vu cong nghe thong tin (so doanh nghiep) giua VNPT TP Ho Chi Minh 
va Trung tam Kinh doanh TP Ho Chi Minh;
		
		---Gan gia tri phan chia doanh thu cho MANV_PTM thu 1
		update ttkd_bsc.ct_bsc_ptm 
						set tyle_huongdt = 0.5
								, ghi_chu = ghi_chu || decode(ghi_chu, null, null, '; ') || 'phan chia doanh thu 05_BB_DN1_DN3 eO5605896, id: ' || id
--								, ghi_chu = 'phan chia doanh thu 05_BB_DN1_DN3 eO5605896, id: ' || id
								, vanban_id = '5605896'
								, thang_luong = 6
					where thang_ptm = 202405 and ma_tb in ('hcm_ca_00100831', 'hcm_ca_00100822', 'hcm_ca_00100842')
		;
		commit;
		rollback;
 ---Code moi xu ly phan chia doanh thu
			---insert REC tinh doanh thu cho nhan vien thu 2, neu khong co nhan vien thu 3
				---th co nhan vien thu 3 update cong thuc TYLE_HUONGDT = 100 - Gia tri can chia
				---Gan MANV_PTM phan chia
				 insert into ttkd_bsc.ct_bsc_ptm (THANG_LUONG, THANG_PTM, MA_GD, MA_KH, MA_TB, SOHOPDONG, DICH_VU, TENKIEU_LD, TEN_TB, DIACHI_LD
																		    , SO_NHA, AP_ID, KHU_ID, PHO_ID, PHUONG_ID, QUAN_ID, TINH_ID, SO_GT, MST, MST_TT, SDT_LH, EMAIL_LH, CHUQUAN_ID
																		    , ID_447, NGAY_BBBG, NGAY_CAT, NGAY_LUUHS_TTKD, NGAY_LUUHS_TTVT, NGAY_SCAN, SCANBOOK_DU, NOP_DU, MIEN_HSGOC
																		    , BS_LUUKHO, THOIHAN_ID, TG_THUE_TU, TG_THUE_DEN, SONGAY_SD, MA_DA, CHU_NHOM, VNP_MOI, GOI_CUOC, NHANVIEN_NHAN_ID
																		    , PBH_NHAN_ID, PBH_NHAN_GOC_ID, DICHVUVT_ID, LOAITB_ID, HDKH_ID, HDTB_ID, KIEUHD_ID, KIEULD_ID, LOAIHD_ID, KIEUTN_ID, KHACHHANG_ID
																		    , THANHTOAN_ID, THUEBAO_ID, THUEBAO_CHA_ID, DOITUONG_ID, DOITUONG_CT_ID, TOCDO_ID, MUCUOCTB_ID, GOI_ID, PHANLOAI_ID, SL_MAILING
																		    , DOIVT_ID, TTVT_ID, MA_TIEPTHI, MA_TIEPTHI_NEW, TO_TT_ID, DONVI_TT_ID, DONVI_TT, NHANVIENGT_ID, MA_NGUOIGT, NGUOI_GT, NHOM_GT, MANV_TT_DAI
																		    , MA_TO_DAI, MA_VTCV_DAI, MANV_HOTRO, TYLE_HOTRO, TYLE_AM, GHI_CHU, LYDO_KHONGTINH_LUONG, KH_ID, MA_DT_KH, PBH_DB_ID, PBH_QL_ID, PBH_PTM_ID
																		    , MA_PB, TEN_PB, MA_TO, TEN_TO, MANV_PTM, TENNV_PTM, MA_VTCV, LOAINV_ID, TEN_LOAINV, LOAI_LD, NHOM_TIEPTHI, DATCOC_CSD, THANG_BDDC, THANG_KTDC, SOTHANG_DC
																		    , CHIPHI_TTKD, THANG_XN_CHIPHI_TTKD, TYLE_HUONGDT, TIEN_DNHM, TIEN_SODEP, TIEN_CAMKET, NGAY_TT, SOSERI, TIEN_TT, HT_TRA_ID, DTHU_PS_TRUOC
																		    , DTHU_PS, DTHU_PS_N1, DTHU_PS_N2, DTHU_PS_N3, TRANGTHAITB_ID, TRANGTHAITB_ID_N1, TRANGTHAITB_ID_N2, TRANGTHAITB_ID_N3, TRANGTHAITB_ID_N5
																		    , NOCUOC_PTM, NOCUOC_N1, NOCUOC_N2, NOCUOC_N3, NOCUOC_T1, NOCUOC_T2, XACNHAN_KHKT, THANG_XACNHAN_KHKT, DOITUONG_KH, KHHH_KHM, DIABAN
																		    , DTHU_GOI_GOC, DTHU_GOI, DTHU_GOI_NGOAIMANG, HESO_DICHVU, HESO_DICHVU_1, PHANLOAI_KH, HESO_KHACHHANG, HESO_TBNGANHAN, HESO_TRATRUOC
																		    , HESO_KHUYENKHICH, HESO_KVDACTHU, HESO_VTCV_NVPTM, HESO_VTCV_DAI, HESO_VTCV_NVHOTRO, HESO_HOTRO_NVPTM, HESO_HOTRO_DAI, HESO_HOTRO_NVHOTRO
																		    , HESO_QUYDINH_NVPTM, HESO_QUYDINH_DAI, HESO_QUYDINH_NVHOTRO, HESO_DIABAN_TINHKHAC, HESO_HOSO, HESO_DICHVU_DNHM, DONGIA, DOANHTHU_DONGIA_NVPTM
																		    , DOANHTHU_KPI_NVPTM, LUONG_DONGIA_NVPTM, DOANHTHU_DONGIA_DAI, DOANHTHU_KPI_NVDAI, LUONG_DONGIA_DAI, DOANHTHU_DONGIA_NVHOTRO, DOANHTHU_KPI_NVHOTRO
																		    , LUONG_DONGIA_NVHOTRO, DOANHTHU_KPI_TO, DOANHTHU_KPI_PHONG, DOANHTHU_KPI_DNHM, DOANHTHU_DONGIA_DNHM, LUONG_DONGIA_DNHM_NVPTM, DOANHTHU_KPI_DNHM_PHONG
																		    , THANG_TLDG_DNHM, THANG_TLKPI_DNHM, THANG_TLKPI_DNHM_TO, THANG_TLKPI_DNHM_PHONG, THANG_TLDG_DT, THANG_TLKPI, THANG_TLDG_DT_NVHOTRO, THANG_TLKPI_HOTRO
																		    , THANG_TLDG_DT_DAI, THANG_TLKPI_TO, THANG_TLKPI_PHONG, LYDO_KHONGTINH_DONGIA, NOCUOC_THUHOI, THANG_TLDG_TH, SOTHANGNO_THUHOI, LINE_ID, LINE_NVAM_QL, MA_DUAN_BANHANG
																		    , KIEMTRA_DUAN, DIABAN_ID, LOAI_TB, VANBAN_ID, NGUON, GOI_LUONGTINH, BS_D500_NEW, USER_CN, TEN_USER_CN, TINH_XUATHANG, PHEDUYET_OK, DUQ_CAP1_DKGOI, GOI_CKDAI, DTHU_KIT_TRATRUOC
																		    , DTHU_NT_THANGKH, DTHU_TKC_THANGKH, TINH_PSTKC_THANGKH, USER_SMCS, LOAI_DIEM_BAN, KENH_NOIBO, THOADK_BSC, THOADK_DG, DONGIA_BH, DONGIA_KK, CHUKY_GOI, TRANGTHAI_TT_ID, HESO_DAILY
																		    , MA_QLDA, UNGDUNG_ID, MA_GD_GT)
				    
					    select '5' thang_luong, THANG_PTM, MA_GD, MA_KH, MA_TB, SOHOPDONG, DICH_VU, TENKIEU_LD, TEN_TB, DIACHI_LD
									    , SO_NHA, AP_ID, KHU_ID, PHO_ID, PHUONG_ID, QUAN_ID, TINH_ID, SO_GT, MST, MST_TT, SDT_LH, EMAIL_LH, CHUQUAN_ID
									    , ID_447, NGAY_BBBG, NGAY_CAT, NGAY_LUUHS_TTKD, NGAY_LUUHS_TTVT, NGAY_SCAN, SCANBOOK_DU, NOP_DU, MIEN_HSGOC
									    , BS_LUUKHO, THOIHAN_ID, TG_THUE_TU, TG_THUE_DEN, SONGAY_SD, MA_DA, CHU_NHOM, VNP_MOI, GOI_CUOC, NHANVIEN_NHAN_ID
									    , PBH_NHAN_ID, PBH_NHAN_GOC_ID, DICHVUVT_ID, LOAITB_ID, HDKH_ID, HDTB_ID, KIEUHD_ID, KIEULD_ID, LOAIHD_ID, KIEUTN_ID, KHACHHANG_ID
									    , THANHTOAN_ID, THUEBAO_ID, THUEBAO_CHA_ID, DOITUONG_ID, DOITUONG_CT_ID, TOCDO_ID, MUCUOCTB_ID, GOI_ID, PHANLOAI_ID, SL_MAILING
									    , DOIVT_ID, TTVT_ID, MA_TIEPTHI, MA_TIEPTHI_NEW, TO_TT_ID, DONVI_TT_ID, DONVI_TT, NHANVIENGT_ID, MA_NGUOIGT, NGUOI_GT, NHOM_GT, MANV_TT_DAI
									    , MA_TO_DAI, MA_VTCV_DAI, MANV_HOTRO, TYLE_HOTRO, TYLE_AM, GHI_CHU, LYDO_KHONGTINH_LUONG, KH_ID, MA_DT_KH, PBH_DB_ID, PBH_QL_ID, PBH_PTM_ID
									    , MA_PB, TEN_PB, MA_TO, TEN_TO, 'CTV082537' MANV_PTM, TENNV_PTM, MA_VTCV, LOAINV_ID, TEN_LOAINV, LOAI_LD, NHOM_TIEPTHI, DATCOC_CSD, THANG_BDDC, THANG_KTDC, SOTHANG_DC
									    , CHIPHI_TTKD, THANG_XN_CHIPHI_TTKD, 1- TYLE_HUONGDT TYLE_HUONGDT, TIEN_DNHM, TIEN_SODEP, TIEN_CAMKET, NGAY_TT, SOSERI, TIEN_TT, HT_TRA_ID, DTHU_PS_TRUOC
									    , DTHU_PS, DTHU_PS_N1, DTHU_PS_N2, DTHU_PS_N3, TRANGTHAITB_ID, TRANGTHAITB_ID_N1, TRANGTHAITB_ID_N2, TRANGTHAITB_ID_N3, TRANGTHAITB_ID_N5
									    , NOCUOC_PTM, NOCUOC_N1, NOCUOC_N2, NOCUOC_N3, NOCUOC_T1, NOCUOC_T2, XACNHAN_KHKT, THANG_XACNHAN_KHKT, DOITUONG_KH, KHHH_KHM, DIABAN
									    , DTHU_GOI_GOC, DTHU_GOI, DTHU_GOI_NGOAIMANG, HESO_DICHVU, HESO_DICHVU_1, PHANLOAI_KH, HESO_KHACHHANG, HESO_TBNGANHAN, HESO_TRATRUOC
									    , HESO_KHUYENKHICH, HESO_KVDACTHU, HESO_VTCV_NVPTM, HESO_VTCV_DAI, HESO_VTCV_NVHOTRO, HESO_HOTRO_NVPTM, HESO_HOTRO_DAI, HESO_HOTRO_NVHOTRO
									    , HESO_QUYDINH_NVPTM, HESO_QUYDINH_DAI, HESO_QUYDINH_NVHOTRO, HESO_DIABAN_TINHKHAC, HESO_HOSO, HESO_DICHVU_DNHM, DONGIA, DOANHTHU_DONGIA_NVPTM
									    , DOANHTHU_KPI_NVPTM, LUONG_DONGIA_NVPTM, DOANHTHU_DONGIA_DAI, DOANHTHU_KPI_NVDAI, LUONG_DONGIA_DAI, DOANHTHU_DONGIA_NVHOTRO, DOANHTHU_KPI_NVHOTRO
									    , LUONG_DONGIA_NVHOTRO, DOANHTHU_KPI_TO, DOANHTHU_KPI_PHONG, DOANHTHU_KPI_DNHM, DOANHTHU_DONGIA_DNHM, LUONG_DONGIA_DNHM_NVPTM, DOANHTHU_KPI_DNHM_PHONG
									    , THANG_TLDG_DNHM, THANG_TLKPI_DNHM, THANG_TLKPI_DNHM_TO, THANG_TLKPI_DNHM_PHONG, THANG_TLDG_DT, THANG_TLKPI, THANG_TLDG_DT_NVHOTRO, THANG_TLKPI_HOTRO
									    , THANG_TLDG_DT_DAI, THANG_TLKPI_TO, THANG_TLKPI_PHONG, LYDO_KHONGTINH_DONGIA, NOCUOC_THUHOI, THANG_TLDG_TH, SOTHANGNO_THUHOI, LINE_ID, LINE_NVAM_QL, MA_DUAN_BANHANG
									    , KIEMTRA_DUAN, DIABAN_ID, LOAI_TB, VANBAN_ID, NGUON, GOI_LUONGTINH, BS_D500_NEW, USER_CN, TEN_USER_CN, TINH_XUATHANG, PHEDUYET_OK, DUQ_CAP1_DKGOI, GOI_CKDAI, DTHU_KIT_TRATRUOC
									    , DTHU_NT_THANGKH, DTHU_TKC_THANGKH, TINH_PSTKC_THANGKH, USER_SMCS, LOAI_DIEM_BAN, KENH_NOIBO, THOADK_BSC, THOADK_DG, DONGIA_BH, DONGIA_KK, CHUKY_GOI, TRANGTHAI_TT_ID, HESO_DAILY
									    , MA_QLDA, UNGDUNG_ID, MA_GD_GT
							  from ttkd_bsc.ct_bsc_ptm
							  where ma_tb in ('hcm_ca_00100831', 'hcm_ca_00100822', 'hcm_ca_00100842')
		  ;
		---update thong tin Nhanvien AM dc phan chia
		update ttkd_bsc.ct_bsc_ptm a
						set (manv_ptm, tennv_ptm, ma_to, ten_to, ma_pb, ten_pb, ma_vtcv, loai_ld, nhom_tiepthi)
						= (select b.ma_nv, b.ten_nv, b.ma_to, b.ten_to, b.ma_pb, b.ten_pb, b.ma_vtcv, b.loai_ld, b.nhomld_id
							  from ttkd_bsc.nhanvien b
							  where b.thang = 202405 --thang n
											and b.ma_nv = a.manv_ptm
							  )  
							  where ma_tb in ('hcm_ca_00100831', 'hcm_ca_00100822', 'hcm_ca_00100842')
											and MANV_PTM = 'CTV082537'
		;
		---END

 select * from ttkd_bsc.ct_ptm_phanchia_dthu;          

update ttkd_bsc.ct_ptm_phanchia_dthu a set (ten_tb, diachi_ld, ngay_bbbg, dichvuvt_id, loaitb_id)=
            (select bb.ten_tb, bb.diachi_ld, bb.ngay_ht, bb.dichvuvt_id, bb.loaitb_id 
				from css_hcm.hd_khachhang aa, css_hcm.hd_thuebao bb
                where aa.hdkh_id=bb.hdkh_id and aa.loaihd_id=1
                            and aa.ma_gd=a.ma_gd and bb.ma_tb=a.ma_tb
					   )
     where ngay_bbbg is null;

update ttkd_bsc.ct_bsc_ptm a set (thang_luong, tyle_huongdt)=
            (select 11, tyle_ttkd from ttkd_bsc.ct_ptm_phanchia_dthu where ma_gd=a.ma_gd and ma_tb=a.ma_tb)
        where thang_ptm='202205' and exists  (select tyle_ttkd from ttkd_bsc.ct_ptm_phanchia_dthu where ma_gd=a.ma_gd and ma_tb=a.ma_tb)
	   ;

       
       
                 -- Doanh thu don gia cac dv ngoai tru vnptt, SMS Brandname:
            update ttkd_bsc.ct_bsc_ptm a set 
                        doanhthu_dongia_nvptm = round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                            *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                            *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                            *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
                        ,doanhthu_dongia_nvhotro =null --dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvhotro*heso_vtcv_nvhotro*heso_hotro_nvhotro*heso_khachhang
                        ,doanhthu_dongia_dai = round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_dai*heso_vtcv_dai*heso_hotro_dai*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ,0)
            -- select thang_ptm, ma_tb, ma_vtcv, dthu_goi, tyle_hotro, heso_dichvu, heso_hotro_nvhotro, heso_quydinh_nvptm,heso_vtcv_nvptm, heso_khuyenkhich, heso_tratruoc, heso_kvdacthu, doanhthu_dongia_nvptm, round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)*nvl(tyle_huongdt,1) ,0) dthu_dongia_new, thang_tldg_dt from ct_bsc_ptm
            where thang_luong='11';
                       
            commit;    
            
                -- Doanh thu goi tich hop: ap dung khong phan biet tap quan ly (theo VB 275/TTr-NS-DH 22/06/2020)
                            -- tham khao bang huong dan cua anh Nghia tinh he so cac goi tich hop cho SMEs: VB 275/TTKD HCM-DH 22/06/2020:
            update ttkd_bsc.ct_bsc_ptm a set doanhthu_dongia_nvptm =
                        (case when goi_id=15599  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.21/0.1434 ,0)  -- SME_NEW
                                    when goi_id=15600  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.25/0.17 ,0)  -- SME+
                                    when goi_id=15602  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.25/0.21 ,0)  -- SME_BASIC 1
                                    when goi_id=15601  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.35/0.30 ,0)  -- SME_BASIC 2   
                                    when goi_id=15604  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.19/0.13 ,0)  -- SME_SMART1
                                    when goi_id=15603  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.20/0.14 ,0)  -- SME_SMART2
                                    when goi_id=15605  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.20/0.16 ,0)  -- F_Pharmacy
                                    when goi_id=15596  then round( (dthu_goi*heso_dichvu*nvl(heso_tratruoc,1)*nvl(heso_kvdacthu,1)
                                                                                        *heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)
                                                                                        *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1) ) * 0.20/0.15 ,0)  -- F_ORM
                         end)                                                                                                                                                       
            -- select ma_tb, dthu_goi, doanhthu_dongia_nvptm, dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1) dthu_dongia_new from ct_bsc_ptm
            where thang_luong='1' and goi_id in (15596,15599,15600,15601,15602,15603,15604,15605);
                        
                        
            commit;                            
                
            
                 -- Luong don gia cac dv ngoai tru vnptt, SMS Brandname:
            update ttkd_bsc.ct_bsc_ptm a set luong_dongia_nvptm=nvl(doanhthu_dongia_nvptm,0)*dongia/1000,
                                                                            luong_dongia_nvhotro = null,  
                                                                            luong_dongia_dai=nvl(doanhthu_dongia_dai,0)*dongia/1000
                 -- select thang_ptm, ma_tb, heso_dichvu, doanhthu_dongia_nvptm, luong_dongia_nvptm from ct_bsc_ptm a
                 where thang_luong='11';
                            
                             
                commit;            
            
            -- SMS Brandname: Tinh lai dthu don gia dv sms brandname cua thang n-1: 
            --ma_tb<>'hcm_brn_00009121';  --  VB 1254 TTr-?H-NS
            update ttkd_bsc.ct_bsc_ptm a set 
                        doanhthu_dongia_nvptm =( (nvl(dthu_goi,0)*heso_dichvu)+(nvl(dthu_goi_ngoaimang,0)*heso_dichvu_1) )
                                                                                *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvptm
                                                                                *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm*nvl(heso_hotro_nvptm,1)
                                                                                *heso_khachhang*nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)
                        ,doanhthu_dongia_nvhotro =null  -- dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)*heso_quydinh_nvhotro*heso_vtcv_nvhotro*heso_hotro_nvhotro*heso_khachhang
                        ,doanhthu_dongia_dai =null
            -- select ma_tb, dthu_goi, dthu_goi_ngoaimang, heso_dichvu, heso_dichvu_1, thang_tldg_dt, thang_tlkpi_phong,lydo_khongtinh_luong from ct_bsc_ptm 
            where thang_luong='5' and loaitb_id=131;
                       
                         
            commit;
            
            update ttkd_bsc.ct_bsc_ptm set luong_dongia_nvptm=nvl(doanhthu_dongia_nvptm,0)*dongia/1000,
                                                                             luong_dongia_nvhotro = null,  -- nvl(doanhthu_dongia_nvhotro,0)*dongia/1000,    
                                                                             luong_dongia_dai=nvl(doanhthu_dongia_dai,0)*dongia/1000
                 where thang_luong='5' and loaitb_id=131;
                           
            commit;
            
  
--====== DOANH THU KPI  CA NHAN =======:

            update ttkd_bsc.ct_bsc_ptm a
                        set doanhthu_kpi_nvptm= round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                            *heso_quydinh_nvptm*nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                            *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                            *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1) *nvl(tyle_huongdt,1),0)
                -- select dich_vu, ma_gd, ma_tb, dthu_goi, doanhthu_dongia_nvptm, heso_dichvu, heso_khachhang, heso_vtcv_nvptm,heso_hotro_nvptm, *nvl(tyle_huongdt,1),doanhthu_kpi_nvptm,lydo_khongtinh_dongia, thang_tldg_dt from ct_bsc_ptm a 
                where thang_luong='11';
                            
            commit;


           -- dthu kpi cua dich vu giai phap thiet bi, giai phap CNTT: 
                -- dthu tinh bsc la dthu hop dong x cac he so tinh bsc (ko tinh tren chenh lech thu chi).
            update ttkd_bsc.ct_bsc_ptm a
                        set doanhthu_kpi_nvptm= round(dthu_goi_goc*nvl(heso_dichvu,1)*nvl(heso_dichvu_1,1)*nvl(heso_quydinh_nvptm,1)
                                                      *decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm) -- *decode(tyle_hotro,null,1,1-tyle_hotro)
                                                      --*decode(manv_tt_dai,null,1,0.5) 
                                                      *nvl(heso_tbnganhan,1)
                                                      *nvl(heso_diaban_tinhkhac,1)  ,0)
                -- select dich_vu, ma_gd, ma_tb, dthu_goi, doanhthu_dongia_nvptm, heso_dichvu, heso_khachhang, heso_vtcv_nvptm,heso_hotro_nvptm, doanhthu_kpi_nvptm,lydo_khongtinh_dongia, thang_tldg_dt from ct_bsc_ptm a 
                where thang_luong='5' and bo_dau(dich_vu) like '%giai phap%';
                            
                            
            commit;
           
           -- kpi goi SME: ko xet heso_quydinh_nvptm vi ko phan biet tap KH
           update ttkd_bsc.ct_bsc_ptm a
                        set doanhthu_kpi_nvptm=round(dthu_goi*nvl(heso_dichvu,1)*nvl(heso_quydinh_nvptm,1)
                                                      *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                      *nvl(heso_kvdacthu,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                      *decode(tyle_hotro,null,1,1-tyle_hotro)*decode(manv_tt_dai,null,1,0.5)  ,0)
                                                      *nvl(heso_diaban_tinhkhac,1)
                where thang_luong='5' and goi_id in (15596,15599,15600,15601,15602,15603,15604,15605);
           commit;
           

               -- kpi nv ho tro:
            update ttkd_bsc.ct_bsc_ptm a
                       set doanhthu_kpi_nvhotro=round(dthu_goi*heso_dichvu*nvl(heso_quydinh_nvhotro,1)
                                                                        *nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                        *nvl(heso_kvdacthu,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                        *nvl(heso_diaban_tinhkhac,1)*tyle_hotro   ,0)
                -- select ma_tb, dthu_goi, manv_hotro, tyle_hotro,  doanhthu_kpi_nvhotro, thang_tlkpi_hotro from ct_bsc_ptm a
                where thang_luong='6' and manv_hotro is not null;
                          
            commit;
            

           -- dthu kpi nv ho tro cua dich vu giai phap thiet bi, giai phap CNTT: 
                -- dthu tinh bsc la dthu hop dong x cac he so tinh bsc (ko tinh tren chenh lech thu chi).
            update ttkd_bsc.ct_bsc_ptm a
                        set doanhthu_kpi_nvhotro= round(dthu_goi_goc*nvl(heso_dichvu,1)*nvl(heso_quydinh_nvhotro,1)*tyle_hotro ,0)
                                                                                *nvl(heso_tbnganhan,1)*nvl(heso_diaban_tinhkhac,1)
                -- select dich_vu, ma_gd, ma_tb, manv_hotro, tyle_hotro, dthu_goi_goc, dthu_goi, doanhthu_dongia_nvptm, heso_dichvu, heso_hotro_nvhotro, doanhthu_kpi_nvptm,doanhthu_kpi_nvhotro, lydo_khongtinh_dongia from ct_bsc_ptm a 
                where thang_luong='5' and bo_dau(dich_vu) like '%giai phap%';
                            
                            
            commit;
            
   
              -- SMS Brandname thang n-1:
            update ttkd_bsc.ct_bsc_ptm
                        set doanhthu_kpi_nvptm= round(  ( (nvl(dthu_goi,0)*nvl(heso_dichvu,1))+(nvl(dthu_goi_ngoaimang,0)*nvl(heso_dichvu_1,1))  )
                                                      *nvl(heso_quydinh_nvptm,1)
                                                      *decode(tyle_hotro,null,1,1-tyle_hotro)*decode(manv_tt_dai,null,1,0.5)  ,0)
                                                      *nvl(heso_diaban_tinhkhac,1)
                -- select dthu_goi, dthu_goi_ngoaimang, doanhthu_kpi_nvptm from ct_bsc_ptm
                where thang_luong='5' and loaitb_id=131;
                            
            commit;
            
              -- SMS Brandname thang n-1 cua nv ho tro:
            update ttkd_bsc.ct_bsc_ptm
               set doanhthu_kpi_nvhotro=round(   ( (nvl(dthu_goi,0)*nvl(heso_dichvu,1))+(nvl(dthu_goi_ngoaimang,0)*nvl(heso_dichvu_1,1))  )
                                                                        *nvl(heso_quydinh_nvhotro,1)*tyle_hotro ,0)*nvl(heso_diaban_tinhkhac,1)
             
             where thang_luong='5' and loaitb_id=131 and manv_hotro is not null ;
            commit;
            
                -- Kpi nv dai:
            update ttkd_bsc.ct_bsc_ptm
                                set doanhthu_kpi_nvdai=doanhthu_dongia_dai
                     -- select * from ct_bsc_ptm
                     where thang_luong='5' and manv_tt_dai is not null; 
                     
            commit;



-- ========= DOANH THU KPI TO, PHONG =========:

            update ttkd_bsc.ct_bsc_ptm a
                        set doanhthu_kpi_to= round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                            *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                            *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                            *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
                -- select dich_vu, ma_gd, ma_tb, dthu_goi, doanhthu_dongia_nvptm, heso_dichvu, doanhthu_kpi_nvptm,doanhthu_kpi_phong, lydo_khongtinh_dongia, thang_tldg_dt from ct_bsc_ptm a 
                where thang_luong='11';
                
            update ttkd_bsc.ct_bsc_ptm a
                        set doanhthu_kpi_phong= round( dthu_goi*heso_dichvu*nvl(heso_khuyenkhich,1)*nvl(heso_tratruoc,1)
                                                                            *nvl(heso_kvdacthu,1)*heso_vtcv_nvptm
                                                                            *nvl(heso_hotro_nvptm,1)*nvl(heso_khachhang,1)*nvl(heso_tbnganhan,1)
                                                                            *nvl(heso_hoso,1)*nvl(heso_diaban_tinhkhac,1)*nvl(tyle_huongdt,1) ,0)
                -- select dich_vu, ma_gd, ma_tb, dthu_goi, doanhthu_dongia_nvptm, heso_dichvu, doanhthu_kpi_nvptm,doanhthu_kpi_phong, lydo_khongtinh_dongia, thang_tldg_dt from ct_bsc_ptm a 
                where thang_luong='11';
                            
            commit;
            

           -- dthu kpi cua dich vu giai phap thiet bi, giai phap CNTT: 
                -- dthu tinh bsc la dthu hop dong x cac he so tinh bsc (ko tinh tren chenh lech thu chi). => dthu_goi_goc
            update ct_bsc_ptm a
                        set doanhthu_kpi_to= round(dthu_goi_goc*nvl(heso_dichvu,1)*nvl(heso_dichvu_1,1)  
                                                      *decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm) *nvl(heso_tbnganhan,1) *nvl(heso_diaban_tinhkhac,1)  ,0)
                                                      
                              doanhthu_kpi_phong= round(dthu_goi_goc*nvl(heso_dichvu,1)*nvl(heso_dichvu_1,1)  
                                                      *decode(heso_hotro_nvptm,null,1,heso_hotro_nvptm) *nvl(heso_tbnganhan,1) *nvl(heso_diaban_tinhkhac,1)  ,0)                      
                -- select dich_vu, ma_gd, ma_tb, dthu_goi_goc, dthu_goi, doanhthu_dongia_nvptm, heso_dichvu, doanhthu_kpi_nvptm,doanhthu_kpi_phong, lydo_khongtinh_dongia from ct_bsc_ptm a 
                where thang_luong='5' and bo_dau(dich_vu) like '%giai phap%';
                            
                            
            commit;
            
            
           -- kpi goi SME: ko xet heso_quydinh_nvptm vi ko phan biet tap KH
           update ttkd_bsc.ct_bsc_ptm a
                        set doanhthu_kpi_phong=round(dthu_goi*nvl(heso_dichvu,1)
                                                      *decode(tyle_hotro,null,1,1-tyle_hotro)*decode(manv_tt_dai,null,1,0.5)  ,0)*nvl(heso_tbnganhan,1)
                                                      *nvl(heso_diaban_tinhkhac,1)
                where thang_luong='1' and goi_id in (15596,15599,15600,15601,15602,15603,15604,15605);
           commit;
           
   
              -- SMS Brandname thang n-1:
            update ttkd_bsc.ct_bsc_ptm
                        set doanhthu_kpi_phong= round(  ( (nvl(dthu_goi,0)*nvl(heso_dichvu,1))+(nvl(dthu_goi_ngoaimang,0)*nvl(heso_dichvu_1,1))  )
                                                      *decode(tyle_hotro,null,1,1-tyle_hotro)*decode(manv_tt_dai,null,1,0.5)  ,0)
                                                      *nvl(heso_diaban_tinhkhac,1)
                -- select dthu_goi, dthu_goi_ngoaimang, doanhthu_kpi_nvptm from ct_bsc_ptm
                where thang_luong='1' and loaitb_id=131;
                            
            commit;
            
          
            ---Code moi xu ly phan chia doanh thu
    select-- *
			thang_ptm, ma_tb, dthu_goi, doanhthu_dongia_nvptm, doanhthu_kpi_nvptm, doanhthu_kpi_to, doanhthu_kpi_phong,
                thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, tyle_huongdt
            from ttkd_bsc.ct_bsc_ptm
            where 
--			ma_tb in ('hcm_tmvn_00004910', 'hcm_web_00009188', 'hcm_ca_00057632', 'hcm_one_business_00000011')
					 ma_tb in ('hcm_ca_00100831', 'hcm_ca_00100822', 'hcm_ca_00100842')
		  ;