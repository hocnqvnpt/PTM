Theo VB 977/VNPT.TPHCM-PTTT ngay 29/3/2022, quy dinh co che kinh te san pham dich vu cong nghe thong tin (so doanh nghiep) giua VNPT TP Ho Chi Minh 
va Trung tam Kinh doanh TP Ho Chi Minh;
		
		---Gan gia tri phan chia doanh thu cho MANV_PTM thu 1
		update ttkd_bsc.ct_bsc_ptm 
						set tyle_huongdt = 0.5
								, ghi_chu = ghi_chu || decode(ghi_chu, null, null, '; ') || 'phan chia doanh thu 746/TTr BHKVSG eO783670, id: ' || id
--								, ghi_chu = 'phan chia doanh thu 05_BB_DN1_DN3 eO5605896, id: ' || id
								, vanban_id = '783670'
								, thang_luong = 3
					where thang_ptm = 202406 and ma_tb in ('hcm_ioff_00000685')
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
				    
					    select thang_luong, THANG_PTM, MA_GD, MA_KH, MA_TB, SOHOPDONG, DICH_VU, TENKIEU_LD, TEN_TB, DIACHI_LD
									    , SO_NHA, AP_ID, KHU_ID, PHO_ID, PHUONG_ID, QUAN_ID, TINH_ID, SO_GT, MST, MST_TT, SDT_LH, EMAIL_LH, CHUQUAN_ID
									    , ID_447, NGAY_BBBG, NGAY_CAT, NGAY_LUUHS_TTKD, NGAY_LUUHS_TTVT, NGAY_SCAN, SCANBOOK_DU, NOP_DU, MIEN_HSGOC
									    , BS_LUUKHO, THOIHAN_ID, TG_THUE_TU, TG_THUE_DEN, SONGAY_SD, MA_DA, CHU_NHOM, VNP_MOI, GOI_CUOC, NHANVIEN_NHAN_ID
									    , PBH_NHAN_ID, PBH_NHAN_GOC_ID, DICHVUVT_ID, LOAITB_ID, HDKH_ID, HDTB_ID, KIEUHD_ID, KIEULD_ID, LOAIHD_ID, KIEUTN_ID, KHACHHANG_ID
									    , THANHTOAN_ID, THUEBAO_ID, THUEBAO_CHA_ID, DOITUONG_ID, DOITUONG_CT_ID, TOCDO_ID, MUCUOCTB_ID, GOI_ID, PHANLOAI_ID, SL_MAILING
									    , DOIVT_ID, TTVT_ID, MA_TIEPTHI, MA_TIEPTHI_NEW, TO_TT_ID, DONVI_TT_ID, DONVI_TT, NHANVIENGT_ID, MA_NGUOIGT, NGUOI_GT, NHOM_GT, MANV_TT_DAI
									    , MA_TO_DAI, MA_VTCV_DAI, MANV_HOTRO, TYLE_HOTRO, TYLE_AM, GHI_CHU, LYDO_KHONGTINH_LUONG, KH_ID, MA_DT_KH, PBH_DB_ID, PBH_QL_ID, PBH_PTM_ID
									    , MA_PB, TEN_PB, MA_TO, TEN_TO, 'CTV021955' MANV_PTM, TENNV_PTM, MA_VTCV, LOAINV_ID, TEN_LOAINV, LOAI_LD, NHOM_TIEPTHI, DATCOC_CSD, THANG_BDDC, THANG_KTDC, SOTHANG_DC
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
							  where ma_tb in ('hcm_ioff_00000685')
		  ;
		---update thong tin Nhanvien AM dc phan chia
		update ttkd_bsc.ct_bsc_ptm a
						set (manv_ptm, tennv_ptm, ma_to, ten_to, ma_pb, ten_pb, ma_vtcv, loai_ld, nhom_tiepthi)
						= (select b.ma_nv, b.ten_nv, b.ma_to, b.ten_to, b.ma_pb, b.ten_pb, b.ma_vtcv, b.loai_ld, b.nhomld_id
							  from ttkd_bsc.nhanvien b
							  where b.thang = 202409 --thang n
											and b.ma_nv = a.manv_ptm
							  )  
							  where ma_tb in ('hcm_ioff_00000685')
											and MANV_PTM = 'CTV021955'
		;
		---END       
       
      

          
            ---Code moi xu ly phan chia doanh thu
    select-- *
			thang_ptm, ma_tb, dthu_goi, doanhthu_dongia_nvptm, doanhthu_kpi_nvptm, doanhthu_kpi_to, doanhthu_kpi_phong,
                thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, tyle_huongdt
            from ttkd_bsc.ct_bsc_ptm
            where 
--			ma_tb in ('hcm_tmvn_00004910', 'hcm_web_00009188', 'hcm_ca_00057632', 'hcm_one_business_00000011')
					 ma_tb in ('hcm_ca_00100831', 'hcm_ca_00100822', 'hcm_ca_00100842')
		  ;