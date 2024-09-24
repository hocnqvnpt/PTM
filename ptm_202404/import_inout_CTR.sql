---Import fiel trong chuong trinh, ngoai chuong trinh                  
                     create table a_ptm_ngoaictr_imp as
                     select TEN_PB, MANV_PTM, TENNV_PTM, DICH_VU dichvu_vt, TENKIEU_LD, ma_gd, ma_tb, MA_KH, TEN_TB SO_HD, TEN_TB ten_kh
                                    , ngay_bbbg NGAY_YC, TEN_PB goi_cuoc_cu, TEN_PB goi_cuoc_moi, DTHU_PS dthu_goi_cu, DTHU_PS dthu_goi_moi, DTHU_GOI, CHIPHI_TTKD CHIPHI_DOITAC
                                    , HESO_DICHVU, HESO_TRATRUOC, HESO_KHACHHANG, HESO_HOSO, HESO_HOTRO_NVHOTRO, MANV_PTM manv_hotro, DIACHI_LD Dien_giai, CAST( NULL AS NUMBER) TRONG_CT
                           -- select *
                     from ttkd_bsc.ct_bsc_ptm
                     where rownum = 1
                     ;
                     truncate table a_ptm_ngoaictr_imp
                     ;
                     DROP table a_ptm_ngoaictr_imp PURGE;
				 
                     update hocnq_ttkd.a_ptm_ngoaictr_imp  set trong_ct = 1 where
                     ;
                     update hocnq_ttkd.a_ptm_ngoaictr_imp set so_hd = regexp_replace(so_hd, '') where REGEXP_INSTR(so_hd, '')>=1
                     ;
                     select so_hd, ASCIISTR( so_hd ), ASCII(''), regexp_replace(so_hd, '') from a_ptm_ngoaictr_imp where REGEXP_INSTR(so_hd, '')>=1;
                     commit;

----imp ds anh Nghia trong/ngoai ctr
				select * from ttkd_bsc.ct_ptm_ngoaictr_imp where thang = 202404 and dichvu_vt = 'VNP tra sau' and TENKIEU_LD != 'Gia han dich vu';
				 select distinct TEN_PB from ttkd_bsc.ct_ptm_ngoaictr_imp where thang = 202404;
				 commit;
				 
				 update ttkd_bsc.ct_ptm_ngoaictr_imp set TEN_PB = replace(ten_pb, 'P.', '') where thang = 202404;
				 
				 update ttkd_bsc.ct_ptm_ngoaictr_imp set dichvu_vt = bo_dau(dichvu_vt);
				 update ttkd_bsc.ct_ptm_ngoaictr_imp set TENKIEU_LD = bo_dau(TENKIEU_LD);
				 
				 select distinct nguon from ttkd_bsc.ct_bsc_ptm where thang_ptm = 202403-- and --Ngoai co che tinh luong Trong co che tinh luong;
				 ;
				 
				 -----1 xu ly VNPts HHM theo file anh Nghia thoihan_id = 1, heso_tbnganhan = heso_dichvu, heso_hoso = heso_hoso
				 select a.thang_ptm, thoihan_id, heso_tbnganhan, a.heso_hoso, a.dthu_goi
							, b.dthu_goi dthu_goi_aNghia, b.HESO_DICHVU, b.HESO_TRATRUOC, b.HESO_KHACHHANG, b.HESO_HOSO HESO_HOSO_n, b.HESO_HOTRO_NVHOTRO, b.MANV_HOTRO, DIEN_GIAI
				 from ttkd_bsc.ct_bsc_ptm a
								join (select * from ttkd_bsc.ct_ptm_ngoaictr_imp 
																where thang = 202404 and dichvu_vt = 'VNP tra sau' and TENKIEU_LD != 'Gia han dich vu'
												) b on a.ma_tb = b.ma_tb and a.ma_gd = b.ma_gd
				 where a.loaitb_id = 20
								and a.dthu_goi <> b.dthu_goi
				;
				MERGE INTO ttkd_bsc.ct_bsc_ptm a
				USING (select ma_gd, MA_TB, DTHU_GOI, HESO_DICHVU, HESO_HOSO, dien_giai from ttkd_bsc.ct_ptm_ngoaictr_imp 
																where thang = 202404 and dichvu_vt = 'VNP tra sau' and TENKIEU_LD <> 'Gia han dich vu'
							) b
							ON (a.ma_tb = b.ma_tb and a.ma_gd = b.ma_gd)
				WHEN MATCHED THEN
						UPDATE SET thang_luong = 18, thoihan_id = 1, heso_tbnganhan = 0.3, heso_hoso = b.heso_hoso  --fix so
												, a.dthu_goi_goc = b.dthu_goi, a.dthu_goi = b.dthu_goi
												, a.ghi_chu = ghi_chu || '; ' || b.dien_giai
						WHERE a.loaitb_id = 20
				;
				commit;
				---2 Xu ly dich vu CNTT ngoai tru VNPTts, INT truc tiep
						--insert het cac cot anh Nghia
						-- bsung dthu_ps, phanloai_kh, MST file giao a Nguyen
				select * 
					from ttkd_bsc.ct_ptm_ngoaictr_imp 
					where thang = 202404 and dichvu_vt not in ('VNP tra sau', 'Internet truc tiep')
					;
				select a.thang_ptm, a.THANG_TLDG_DT, a.ma_tb, thoihan_id, heso_tbnganhan, a.heso_hoso, a.dthu_goi, a.ma_gd
							, b.trong_ct, b.ma_gd ma_gd_aNghia, b.dthu_goi dthu_goi_aNghia, b.HESO_DICHVU, b.HESO_TRATRUOC, b.HESO_KHACHHANG, b.HESO_HOSO HESO_HOSO_n, b.HESO_HOTRO_NVHOTRO, b.MANV_HOTRO, DIEN_GIAI
				 from ttkd_bsc.ct_bsc_ptm a
								join (select * from ttkd_bsc.ct_ptm_ngoaictr_imp 
																where thang = 202404 and dichvu_vt not in ('VNP tra sau', 'Internet truc tiep')
												) b on a.ma_tb = b.ma_tb --and a.ma_gd = b.ma_gd
				-- where a.loaitb_id = 20
				;
				update ttkd_bsc.ct_ptm_ngoaictr_imp set command = 'insert' 
												--	, loaitb_id = 999 
												, dthu_goi = 3540000
												, ma_gd = 'HCM-DV/10331282'
				where thang = 202404 and --loaitb_id = 39--
																--DICHVU_VT= 'Voice Brandname' and ma_gd = 'HCM-LD/01592713'
																ma_tb = 'hcm_ioff_00000621'
																;
				;
				commit;
				alter table ttkd_bsc.ct_ptm_ngoaictr_imp add (thuebao_id number);
				update ttkd_bsc.ct_ptm_ngoaictr_imp a set --dichvuvt_id = 16  
								thuebao_id = (select thuebao_id from css_hcm.db_thuebao where ma_tb = a.ma_tb and loaitb_id = a.loaitb_id)
				where thang = 202404 --and loaitb_id = 999
				;
				select pbh_ptm_id from ttkd_bsc.ct_bsc_ptm where thang_ptm = 202404;
				desc ttkd_bsc.ct_bsc_ptm;
				--- sau insert va update xong chay file 
				insert into ttkd_bsc.ct_bsc_ptm (thang_luong, thang_ptm, ten_pb, ma_pb, ten_to, ma_to, manv_ptm, tennv_ptm, ma_vtcv, loai_ld, NHOM_TIEPTHI, TENKIEU_LD, ma_gd, ma_tb, ma_kh
																		, SOHOPDONG, ten_tb, NGAY_BBBG, GOI_CUOC, DTHU_GOI, HESO_DICHVU, HESO_HOTRO_NVHOTRO, MANV_HOTRO, tyle_hotro, GHI_CHU
																		, LOAITB_ID, DICHVUVT_ID, DOITUONG_KH, THUEBAO_ID, khachhang_id, nguon, PHANLOAI_KH, MST
																		, DTHU_PS, trangthaitb_id, chuquan_id, dongia, dich_vu, nop_du)
							select 86, THANG, b.ten_pb, ma_pb, ten_to, ma_to, MANV_PTM, TENNV_PTM, ma_vtcv, b.loai_ld, b.NHOMLD_ID NHOM_TIEPTHI, TENKIEU_LD, a.MA_GD, nvl(c.MA_TB, 'khongco'||rownum) ma_tb, kh.MA_KH, a.SO_HD
											, kh.TEN_KH, a.NGAY_YC, GOI_CUOC_MOI, DTHU_GOI, HESO_DICHVU
											, HESO_HOTRO_NVHOTRO, MANV_HOTRO, HESO_HOTRO_NVHOTRO tyle_hotro, DIEN_GIAI, a.LOAITB_ID, a.DICHVUVT_ID, a.DOITUONG_KH, a.THUEBAO_ID, c.khachhang_id
											, 'ct_ptm_ngoaictr_imp_'||command|| '_' ||thang as nguon, plk.MA_PLKH, 1 mst, dthu_ps, nvl(c.trangthaitb_id, 1), 145 chuquan_id, 858 dongia, lh.loaihinh_tb
											, 1 nop_du, 1 mien_hsgoc, 1 trangthai_tt_id
								from ttkd_bsc.ct_ptm_ngoaictr_imp a
											left join ttkd_bsc.nhanvien_202404 b on a.MANV_PTM = b.ma_nv
											left join css_hcm.db_thuebao c on a.thuebao_id = c.thuebao_id
											left join css_hcm.db_khachhang kh on c.khachhang_id = kh.khachhang_id
											left join (select x.thuebao_id, plkh_id, sum(dthu) dthu_ps
																	from ttkd_bct.db_thuebao_ttkd x 
																		join ttkd_bct.cuoc_thuebao_ttkd y on x.tb_id = y.tb_id 
																group by x.thuebao_id, plkh_id) db on a.thuebao_id = db.thuebao_id
											left join css_hcm.phanloai_kh plk on db.plkh_id = plk.PHANLOAIKH_ID
											left join css_hcm.loaihinh_tb lh on a.LOAITB_ID = lh.LOAITB_ID
								where thang = 202404 and dichvu_vt not in ('VNP tra sau', 'Internet truc tiep')
												and command = 'insert'
												--and not exists (select 1 from ttkd_bsc.ct_bsc_ptm where thang_ptm = a.thang and nguon like 'ct_ptm_ngoaictr_imp%' and ma_tb = nvl(c.MA_TB, 'khongco'||rownum))
												--and a.ma_tb = 'hcm_ioff_00000621'
					;
					update ttkd_bsc.ct_bsc_ptm set dthu_ps = dthu_goi 
					---select * from  ttkd_bsc.ct_bsc_ptm 
					where dthu_ps is null and nguon like 'ct_ptm_ngoaictr_imp%'  and thang_luong = 86;
--					update ttkd_bsc.ct_bsc_ptm a set trangthaitb_id = (select trangthaitb_id from css_hcm.db_thuebao where thuebao_id = a.thuebao_id) where  nguon = 'ct_ptm_ngoaictr_imp' and thang_luong = 86;
					commit;
					rollback;
					;
					insert into ttkd_bsc.ct_bsc_ptm (thang_luong, thang_ptm, ten_pb, ma_pb, ten_to, ma_to, manv_ptm, tennv_ptm, ma_vtcv, loai_ld, NHOM_TIEPTHI, TENKIEU_LD, ma_gd, ma_tb, ma_kh
																		, SOHOPDONG, ten_tb, NGAY_BBBG, GOI_CUOC, DTHU_GOI, HESO_DICHVU, HESO_HOTRO_NVHOTRO, MANV_HOTRO, GHI_CHU
																		, LOAITB_ID, DICHVUVT_ID, DOITUONG_KH, THUEBAO_ID, nguon, PHANLOAI_KH, MST
																		, DTHU_PS, );
					with t as (
							select 
										THANG, a.MA_GD, a.SO_HD
											, GOI_CUOC_MOI, DTHU_GOI, HESO_DICHVU, HESO_TRATRUOC, HESO_KHACHHANG, HESO_HOSO
											, HESO_HOTRO_NVHOTRO, MANV_HOTRO, DIEN_GIAI, a.THUEBAO_ID
											, 'ct_ptm_ngoaictr_imp_202404' nguon, dthu_ps, chuquan_id, null lydo_khongtinh_luong
								from ttkd_bsc.ct_ptm_ngoaictr_imp a
											left join ttkd_bsc.nhanvien_202404 b on a.MANV_PTM = b.ma_nv
											left join css_hcm.db_thuebao c on a.thuebao_id = c.thuebao_id
											left join css_hcm.db_khachhang kh on c.khachhang_id = kh.khachhang_id
											left join (select x.thuebao_id, plkh_id, sum(dthu) dthu_ps
																	from ttkd_bct.db_thuebao_ttkd x 
																		join ttkd_bct.cuoc_thuebao_ttkd y on x.tb_id = y.tb_id 
																group by x.thuebao_id, plkh_id) db on a.thuebao_id = db.thuebao_id
											left join css_hcm.phanloai_kh plk on db.plkh_id = plk.PHANLOAIKH_ID
								where thang = 202404 and dichvu_vt not in ('VNP tra sau', 'Internet truc tiep')
												and command = 'update'
								)
								select * from t
						;		
					MERGE INTO ttkd_bsc.ct_bsc_ptm a
					USING (
							select 
										THANG, a.MA_GD, a.SO_HD
											, GOI_CUOC_MOI, DTHU_GOI, HESO_DICHVU, HESO_TRATRUOC, HESO_KHACHHANG, HESO_HOSO
											, HESO_HOTRO_NVHOTRO, MANV_HOTRO, DIEN_GIAI, a.THUEBAO_ID
											, 'ct_ptm_ngoaictr_imp' nguon, dthu_ps, c.trangthaitb_id, command
								from ttkd_bsc.ct_ptm_ngoaictr_imp a
											left join ttkd_bsc.nhanvien_202404 b on a.MANV_PTM = b.ma_nv
											left join css_hcm.db_thuebao c on a.thuebao_id = c.thuebao_id
											left join css_hcm.db_khachhang kh on c.khachhang_id = kh.khachhang_id
											left join (select x.thuebao_id, plkh_id, sum(dthu) dthu_ps
																	from ttkd_bct.db_thuebao_ttkd x 
																		join ttkd_bct.cuoc_thuebao_ttkd y on x.tb_id = y.tb_id 
																group by x.thuebao_id, plkh_id) db on a.thuebao_id = db.thuebao_id
											left join css_hcm.phanloai_kh plk on db.plkh_id = plk.PHANLOAIKH_ID
								where thang = 202404 and dichvu_vt not in ('VNP tra sau', 'Internet truc tiep')
												and command = 'update'
								) t
					ON (t.thuebao_id = a.thuebao_id and t.ma_gd = a.ma_gd)
					WHEN MATCHED THEN
						UPDATE SET a.sohopdong = t.so_hd, a.goi_cuoc = t.goi_cuoc_moi, a.dthu_goi = t.dthu_goi
												, a.heso_dichvu = nvl(t.heso_dichvu, a.heso_dichvu)
												, a.heso_tratruoc = nvl(t.heso_tratruoc, a.heso_tratruoc)
												, a.heso_khachhang = nvl(t.heso_khachhang, a.heso_khachhang)
												, a.heso_hoso = nvl(t.heso_hoso, a.heso_hoso)
												, a.heso_hotro_nvhotro = nvl(t.heso_hotro_nvhotro, a.heso_hotro_nvhotro)
												, a.tyle_hotro = nvl(t.heso_hotro_nvhotro, a.heso_hotro_nvhotro)
												, a.ghi_chu = a.ghi_chu || '; ' || t.dien_giai
												, a.nguon = 'ct_ptm_ngoaictr_imp_'||command|| '_' ||thang as nguon
												, a.dthu_ps = nvl(t.dthu_ps, a.dthu_ps)
												, a.trangthaitb_id = nvl(t.trangthaitb_id, a.trangthaitb_id)
												, a.chuquan_id = 145
												, a.lydo_khongtinh_luong = null
												, a.lydo_khongtinh_dongia = null
						WHERE a.thuebao_id in (select thuebao_id from ttkd_bsc.ct_ptm_ngoaictr_imp where thang = 202404 and command = 'update')
					;
					commit;
					rollback;
					---check cot de UPDATE
					select THANG_ptm, TEN_PB, MA_PB, TEN_TO, MA_TO, MANV_PTM, TENNV_PTM
									, MA_VTCV, LOAI_LD, NHOM_TIEPTHI, TENKIEU_LD, MA_GD, MA_TB, MA_KH, sohopdong SO_HD
									,  ten_tb TEN_KH,  ngay_bbbg NGAY_YC, GOI_CUOC GOI_CUOC_MOI, DTHU_GOI, HESO_DICHVU
									, HESO_TRATRUOC, HESO_KHACHHANG, HESO_HOSO, HESO_HOTRO_NVHOTRO
									, MANV_HOTRO,  ghi_chu DIEN_GIAI, LOAITB_ID, DICHVUVT_ID, DOITUONG_KH, THUEBAO_ID, NGUON, phanloai_kh, MST, DTHU_PS
									, thang_tldg_dt, chuquan_id
					from ttkd_bsc.ct_bsc_ptm where nguon like 'ct_ptm_ngoaictr_imp%';insert%'
						;
						---loc cac th ngoai chuong trinh
						select tenkieu_ld, thang_luong, thang_ptm, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, LYDO_KHONGTINH_DONGIA, LUONG_DONGIA_NVPTM
						from ttkd_bsc.ct_bsc_ptm where nguon like 'ct_ptm_ngoaictr_imp%'--insert%'
						;
						update ttkd_bsc.ct_bsc_ptm a
										set THANG_TLDG_DT = 202404, THANG_TLKPI = 202404
													, THANG_TLKPI_TO = 202404, THANG_TLKPI_PHONG = 202404
													, LYDO_KHONGTINH_DONGIA = null, LUONG_DONGIA_NVPTM = null
													, thang_luong = 86, chuquan_id = 145
													, tyle_hotro = HESO_HOTRO_NVHOTRO
												--	dich_vu = (select loaihinh_tb from css_hcm.loaihinh_tb where loaitb_id = a.loaitb_id)
						where nguon like 'ct_ptm_ngoaictr_imp%' and thang_ptm = 202404 
						;
						commit;
						rollback;
						select* from css_hcm.chuquan