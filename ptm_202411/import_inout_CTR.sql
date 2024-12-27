Diều kiện ngoài chuong trình:
-- tính BSC: Hệ sinh thái giáo dục (Edu), CA, HDDT, thu 1 lần, các th khác thì không tính
-- gia hạn Internet trực tiếp khong xét QLDA
---Import fiel trong chuong trinh, ngoai chuong trinh                  

             
                     update ttkd_bsc.ct_ptm_ngoaictr_imp set so_hd = regexp_replace(so_hd, '') where REGEXP_INSTR(so_hd, '')>=1 and thang = 202407
                     ;
                     select so_hd, ASCIISTR( so_hd ), ASCII(''), regexp_replace(so_hd, '') from a_ptm_ngoaictr_imp where REGEXP_INSTR(so_hd, '')>=1;
                     commit;

----imp ds anh Nghia trong/ngoai ctr
				select * from ttkd_bsc.ct_ptm_ngoaictr_imp where thang = 202407 and nvl(dichvuvt_id, 0)  in (14, 15, 16, 0) ; and ma_tb = 'hcm_colo_00010493'; and dichvu_vt = 'VNP tra sau' and TENKIEU_LD != 'Gia han dich vu';
				 select distinct TEN_PB from ttkd_bsc.ct_ptm_ngoaictr_imp where thang = 202404;
				 commit;
				 rollback;
				 
				 update ttkd_bsc.ct_ptm_ngoaictr_imp set TEN_PB = replace(ten_pb, 'P.', '') where instr(ten_pb, 'P.') >0 and thang = 202405;
				 update ttkd_bsc.ct_ptm_ngoaictr_imp set dichvu_vt = bo_dau(dichvu_vt) where thang = 202405;
				 update ttkd_bsc.ct_ptm_ngoaictr_imp set TENKIEU_LD = bo_dau(TENKIEU_LD) where thang = 202405;
				 update ttkd_bsc.ct_ptm_ngoaictr_imp set ma_gd = regexp_replace(ma_gd, ' ', '') where thang = 202407 and nvl(dichvuvt_id, 0)  in (14, 15, 16, 0);
				 
				 select 'aa'|| chr(13)||'ggg'from dual;
				 select distinct nguon from ttkd_bsc.ct_bsc_ptm where thang_ptm = 202403-- and --Ngoai co che tinh luong Trong co che tinh luong;
				 ;
		
				---2 Xu ly dich vu CNTT ngoai tru VNPTts, INT truc tiep
						--insert het cac cot anh Nghia
						-- bsung dthu_ps, phanloai_kh, MST file giao a Nguyen
			
				select a.thang_ptm, a.THANG_TLDG_DT, a.ma_tb, thoihan_id, heso_tbnganhan, a.heso_hoso, a.dthu_goi, a.ma_gd
							, b.trong_ct, b.ma_gd ma_gd_aNghia, b.dthu_goi dthu_goi_aNghia, b.HESO_DICHVU, b.HESO_TRATRUOC, b.HESO_KHACHHANG, b.HESO_HOSO HESO_HOSO_n, b.HESO_HOTRO_NVHOTRO, b.MANV_HOTRO, DIEN_GIAI
				 from ttkd_bsc.ct_bsc_ptm a
								join (select * from ttkd_bsc.ct_ptm_ngoaictr_imp 
																where thang = 202405 and dichvu_vt not in ('VNP tra sau', 'Internet truc tiep')
												) b on a.ma_tb = b.ma_tb --and a.ma_gd = b.ma_gd
				-- where a.loaitb_id = 20
				;
			
				---update VNP
				MERGE INTO ttkd_bsc.ct_ptm_ngoaictr_imp a
				USING css_hcm.db_thuebao b
				ON 	('84' || b.ma_tb = a.ma_tb and b.loaitb_id = a.loaitb_id)
				WHEN MATCHED THEN
					UPDATE
					SET a.thuebao_id = b.thuebao_id
					WHERE a.thang = 202405 and a.loaitb_id = 20
					;
				---update ngoai VNP
				MERGE INTO ttkd_bsc.ct_ptm_ngoaictr_imp a
				USING css_hcm.db_thuebao b
				ON 	(b.ma_tb = a.ma_tb and b.loaitb_id = a.loaitb_id)
				WHEN MATCHED THEN
					UPDATE
					SET a.thuebao_id = b.thuebao_id
					WHERE a.thang = 202406 and a.ma_tb = 'hcm_smd_00009024'
					;
				
				commit;
				rollback;
				
					update ttkd_bsc.ct_ptm_ngoaictr_imp set
--										command = 'existed'
--										command = 'thanhly' 
										command = 'insert'
--										command = 'update'
--										command = 'khongtinh' 
--													,loaitb_id = 999, dichvuvt_id = 16,
										, loaitb_id =180,
--												, dthu_goi = round(4645615/1.1, 0)
--												, ma_gd = 'HCM-CQ/13636831'
--												ma_tb = 'hcm_vbn_00001181'
												doituong_kh = 'KHDN'
--								HESO_HOSO = 0.8
--								,  ghichu = ghichu || '; thu hoi T05, T06, tính bsung T10'
--								, DIEN_GIAI = DIEN_GIAI || '; ' ||GHICHU
--									, ghichu = 'Hệ số hồ sơ = 0.8 (P2 có VB số 6582/KHDN2 ngày 26/11/2024)'
--									, TENKIEU_LD = 'Mua them dich vu'
--								mst = '0301239352'
--, thang = 202408
--				select * from ttkd_bsc.ct_ptm_ngoaictr_imp 
				where thang = 202411 --and dichvuvt_id in (14, 15, 16)
--							ma_tb = 'hcm_vbn_00001181'
--							and DICHVU_VT = 'ID Check'
--							and thuebao_id = 12574448 --chu upda, dang kiem tra vi sao tien hok c� trong ctr
--							and thuebao_id in (12208645) 
--							and ma_gd in ('00998559')
--							and ma_tb = 'hcm_vbn_00001078'
							and rowid in ( 'AAHL0jABmAAAaElAAA')
--							and ma_duan_banhang = '237001'
--							and doituong_kh is null
--							and manv_hotro = 'VNP017373; D?ch v? ch?a c� tr�n oneBSS'--is not null
				;		

				--ktra ma_gd da tinh tien
				select * from ttkd_bsc.ct_bsc_ptm where thang_ptm >= 202404 and  ma_tb = 'hcm_edu_lms_00001045';
				
				select dthu_goi, luong_dongia_nvptm, thang_tldg_dt, ma_tb, thuebao_id from ttkd_bsc.ct_bsc_ptm where thang_ptm >= 202404 and replace(ma_gd, ' ', '') = '00998559';'00928721'; 00927583


				select * from ttkd_bsc.ct_ptm_ngoaictr_imp 
								where thang = 202411 and nvl(dichvuvt_id, 0)  in (7, 13, 14, 15, 16, 0) 
												and command is   null ;and ghichu is not null;= 'update'; and ma_tb= 'hcm_vbn_00001065';

				select * from ttkd_bsc.ct_bsc_ptm where  thang_luong in ( 86, 87);
				
				desc ttkd_bsc.ct_bsc_ptm;
				--- Internet Truc tiep & Smartcloud khong tinh dthu KPI
				--- sau insert va update xong chay file 
				insert into ttkd_bsc.ct_bsc_ptm (thang_luong, thang_ptm, ten_pb, ma_pb, ten_to, ma_to, manv_ptm, tennv_ptm, ma_vtcv, loai_ld, NHOM_TIEPTHI, TENKIEU_LD, ma_gd, ma_tb, ma_kh
																		, SOHOPDONG, ten_tb, NGAY_BBBG, GOI_CUOC, DTHU_GOI, HESO_DICHVU, HESO_HOTRO_NVHOTRO, MANV_HOTRO, tyle_hotro, GHI_CHU
																		, LOAITB_ID, DICHVUVT_ID, DOITUONG_KH, THUEBAO_ID, khachhang_id, thanhtoan_id, nguon, MST, MST_TT
																		, DTHU_PS, trangthaitb_id, chuquan_id, dongia, dich_vu, nop_du, mien_hsgoc, trangthai_tt_id, hdkh_id, hdtb_id, loaihd_id, kieuld_id
																		, ma_duan_banhang)
									
							select 86, a.THANG, b.ten_pb, b.ma_pb, b.ten_to, b.ma_to, a.MANV_PTM, a.TENNV_PTM, b.ma_vtcv, b.loai_ld, b.NHOMLD_ID NHOM_TIEPTHI
											, nvl(kld.TEN_KIEULD, TENKIEU_LD) TEN_KIEULD , a.MA_GD, nvl(c.MA_TB, 'khongco'||rownum) ma_tb, nvl(kh.MA_KH, 'GTGT rieng') MA_KH, a.SO_HD
											, nvl(kh.TEN_KH, a.ten_kh) ten_kh, a.NGAY_YC, a.GOI_CUOC_MOI, a.DTHU_GOI, a.HESO_DICHVU
											, HESO_HOTRO_NVHOTRO, MANV_HOTRO, HESO_HOTRO_NVHOTRO tyle_hotro, DIEN_GIAI ||'; ' ||a.GHICHU GHI_CHU, a.LOAITB_ID, a.DICHVUVT_ID, a.DOITUONG_KH, a.THUEBAO_ID, c.khachhang_id, tt.thanhtoan_id
											, 'ct_ptm_ngoaictr_imp_'||command as nguon, nvl(a.mst, kh.mst) mst, tt.mst mst_tt, nvl(db.dthu_ps, a.DTHU_GOI) dthu_ps, nvl(c.trangthaitb_id, 1) trangthaitb_id, 145 chuquan_id, null dongia, nvl(lh.loaihinh_tb, dichvu_vt) dich_vu
											, 1 nop_du, 1 mien_hsgoc			---khong co ma_gd
--											, null nop_du, null mien_hsgoc		--co ma_gd
											, 1 trangthai_tt_id
											, hdkh.hdkh_id, hdtb.hdtb_id, nvl(hdkh.loaihd_id, 1) loaihd_id, nvl(hdtb.kieuld_id, 13266) kieuld_id, a.ma_duan_banhang
								from ttkd_bsc.ct_ptm_ngoaictr_imp a
											left join ttkd_bsc.nhanvien b on b.thang = a.thang and a.MANV_PTM = b.ma_nv
											left join css_hcm.db_thuebao c on a.thuebao_id = c.thuebao_id
											left join css_hcm.db_khachhang kh on c.khachhang_id = kh.khachhang_id
											left join css_hcm.db_thanhtoan tt on tt.thanhtoan_id = c.thanhtoan_id
											left join (select x.thuebao_id, plkh_id, sum(dthu) dthu_ps
																	from ttkd_bct.db_thuebao_ttkd x 
																		join ttkd_bct.cuoc_thuebao_ttkd y on x.tb_id = y.tb_id 
																group by x.thuebao_id, plkh_id) db on a.thuebao_id = db.thuebao_id
--											left join css_hcm.phanloai_kh plk on db.plkh_id = plk.PHANLOAIKH_ID
											left join css_hcm.loaihinh_tb lh on a.LOAITB_ID = lh.LOAITB_ID
											left join css_hcm.hd_khachhang hdkh on a.ma_gd = hdkh.ma_gd
											left join css_hcm.hd_thuebao hdtb on hdkh.hdkh_id = hdtb.hdkh_id and a.thuebao_id = hdtb.thuebao_id
											left join css_hcm.kieu_ld kld on kld.kieuld_id = hdtb.kieuld_id
								where a.thang = 202411 and a.dichvuvt_id in (14, 15, 16)-- not in ('VNP tra sau', 'Internet truc tiep')
												and command = 'insert' 
												and not exists (select 1 from ttkd_bsc.ct_bsc_ptm where thang_ptm = a.thang and nguon like 'ct_ptm_ngoaictr_imp%' and ma_gd = a.ma_gd and ma_tb = nvl(c.MA_TB, 'khongco'||rownum))
												and a.ma_gd  in ('00998559')
												--and a.thuebao_id in (8328877, 8592878)
--												and a.ma_gd is not null
--												and a.rowid in ( 'AAHL0jABmAAAaElAAA')
					;
					
					commit;
					rollback;
					;					
					
					MERGE INTO ttkd_bsc.ct_bsc_ptm a
					USING (
							select 
										a.THANG, a.MA_GD, a.SO_HD
											, GOI_CUOC_MOI, DTHU_GOI, HESO_DICHVU, HESO_TRATRUOC, HESO_KHACHHANG, HESO_HOSO
											, HESO_HOTRO_NVHOTRO, MANV_HOTRO, DIEN_GIAI, a.THUEBAO_ID
											, 'ct_ptm_ngoaictr_imp_'||command as nguon, dthu_ps, c.trangthaitb_id, command, a.ghichu, a.ma_duan_banhang
											, 1 nop_du, 1 mien_hsgoc, 1 trangthai_tt_id
								from ttkd_bsc.ct_ptm_ngoaictr_imp a
											left join ttkd_bsc.nhanvien b on a.thang = b.thang and a.MANV_PTM = b.ma_nv
											left join css_hcm.db_thuebao c on a.thuebao_id = c.thuebao_id
											left join css_hcm.db_khachhang kh on c.khachhang_id = kh.khachhang_id
											left join (select x.thuebao_id, plkh_id, sum(dthu) dthu_ps
																	from ttkd_bct.db_thuebao_ttkd x 
																		join ttkd_bct.cuoc_thuebao_ttkd y on x.tb_id = y.tb_id 
																group by x.thuebao_id, plkh_id) db on a.thuebao_id = db.thuebao_id
											left join css_hcm.phanloai_kh plk on db.plkh_id = plk.PHANLOAIKH_ID
								where a.thang = 202411 and a.dichvuvt_id in (13, 14, 15, 16, 7)
												and command = 'update'
								) t
					ON (t.thuebao_id = a.thuebao_id and t.ma_gd = a.ma_gd)
					WHEN MATCHED THEN
						UPDATE SET a.sohopdong = t.so_hd, a.goi_cuoc = t.goi_cuoc_moi, a.dthu_goi = t.dthu_goi
--												, a.heso_dichvu = nvl(t.heso_dichvu, a.heso_dichvu)
--												, a.heso_tratruoc = nvl(t.heso_tratruoc, a.heso_tratruoc)
--												, a.heso_khachhang = nvl(t.heso_khachhang, a.heso_khachhang)
--												, a.heso_hoso = nvl(t.heso_hoso, a.heso_hoso)
--												, a.heso_hotro_nvhotro = nvl(t.heso_hotro_nvhotro, a.heso_hotro_nvhotro)
--												, a.tyle_hotro = nvl(t.heso_hotro_nvhotro, a.heso_hotro_nvhotro)
												, a.ghi_chu = a.ghi_chu || '; ' || t.dien_giai || '; ' || t.ghichu
												, a.nguon = a.nguon || '; ' || t.nguon
												, a.dthu_ps = nvl(t.dthu_ps, a.dthu_ps)
												, a.trangthaitb_id = nvl(t.trangthaitb_id, a.trangthaitb_id)
												, a.trangthai_tt_id = t.trangthai_tt_id
												, a.ma_duan_banhang = t.ma_duan_banhang
												, a.chuquan_id = 145
												, a.lydo_khongtinh_luong = null
												, a.lydo_khongtinh_dongia = null
												, a.thang_luong = 87
												, THANG_TLDG_DT = null, THANG_TLKPI = null
												, THANG_TLKPI_TO = null, THANG_TLKPI_PHONG = null
												, THANG_TLDG_DT_NVHOTRO = null, THANG_TLKPI_HOTRO = null, THANG_TLDG_DT_DAI = null
						WHERE a.thuebao_id in (select thuebao_id from ttkd_bsc.ct_ptm_ngoaictr_imp where thang = 202411 and command = 'update')
					;
					commit;
					rollback;
				
						update ttkd_bsc.ct_bsc_ptm a
										set THANG_TLKPI_DNHM = 0, THANG_TLKPI_DNHM_TO = 0, THANG_TLKPI_DNHM_PHONG = 0
												, THANG_TLKPI = 0, THANG_TLKPI_HOTRO = 0, THANG_TLKPI_TO = 0, THANG_TLKPI_PHONG = 0
												--	dich_vu = (select loaihinh_tb from css_hcm.loaihinh_tb where loaitb_id = a.loaitb_id)
						where nguon like 'ct_ptm_ngoaictr_imp%' and ghi_chu = 'Khong tinh BSC' and thang_luong in (86,87)
						;
						commit;
						rollback;
						select* from css_hcm.chuquan