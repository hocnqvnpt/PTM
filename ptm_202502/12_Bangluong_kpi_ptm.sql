alter table ttkd_bsc.bangluong_kpi read only;
alter table ttkd_bsc.bangluong_kpi read write;
/*
create table bangluong_kpi_202502_l6 as select * from bangluong_kpi_202502;  


ktra sau khi add du lieu thang do Xuan Vinh cung cap
select * from ttkd_bsc.dinhmuc_dthu_ptm where  thang=202502 ;
update ttkd_bsc.dinhmuc_dthu_ptm set  thang=202502 where thang is null;
update ttkd_bsc.dinhmuc_dthu_ptm set dt_giao_bsc='' where thang=202502 and dt_giao_bsc=0;
*/
create table ttkd_bsc.bangluong_kpi_202502_dot2 as select * from ttkd_bsc.bangluong_kpi_202502;  
create table ttkd_bsc.bangluong_kpi_20250222_dot3 as select * from ttkd_bsc.bangluong_kpi where thang = 202502;  
select * from ttkd_bsc.bangluong_kpi_dot2_20250220 where thang = 202502;  

select distinct a.*, b.*, c.ten_vtcv 
		from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien c
        where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv and c.thang= 202502
                    and a.thang = b.thang and a.thang = c.thang
                    and a.ma_kpi='HCM_DT_PTMOI_021' ;

--      select * from ttkd_bsc.temp_trasau_canhan     
          select * from ttkd_bsc.temp_trasau_canhan;
		drop table ttkd_bsc.temp_trasau_canhan purge;
		desc ttkd_bsc.ghtt_vnpts;
		;5541.9314985 -- 5522.3471035 -- 5522.4071035 -- 5522.4071035
		select sum(dthu_kpi)/1000000
		from ttkd_bsc.temp_trasau_canhan a
							join ttkd_bsc.nhanvien nv on nv.thang = 202502 and a.ma_nv = nv.ma_nv
		where nv.ma_pb in ('VNP0701100', 'VNP0701200', 'VNP0701300', 'VNP0701400', 'VNP0701500','VNP0701600', 'VNP0701800', 'VNP0702100', 'VNP0702200', 'VNP0703000', 'VNP0702300', 'VNP0702400', 'VNP0702500')
							 ;
		----Tat ca dich vu, ngoai tru VNPtt
		create table ttkd_bsc.temp_trasau_canhan as
--		insert into ttkd_bsc.temp_trasau_canhan --(MANV_PTM, DTHU_KPI)
				select DICH_VU, LOAITB_ID, dichvuvt_id, cast(manv_ptm as varchar(20)) ma_nv,  sum(dthu_kpi) dthu_kpi, nguon
				from (
					
						---DNHM cho cot NVPTM
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_ptm, doanhthu_kpi_dnhm * heso_hotro_nvptm dthu_kpi, cast('vb353_ptm' as varchar(20)) nguon
						from ttkd_bsc.ct_bsc_ptm
						where thang_tlkpi_dnhm = 202502 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_dnhm >0
					union all
					---DNHM cho cot NVHOTRO kenh ngoai
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_hotro, doanhthu_kpi_dnhm * heso_hotro_nvhotro doanhthu_kpi_dnhm, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm
						where thang_tlkpi_hotro = 202502 
									and tyle_am is null and tyle_hotro is null and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_dnhm >0
					union all
					---DNHM cho cot DAI
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_tt_dai, doanhthu_kpi_dnhm * heso_hotro_dai doanhthu_kpi_dnhm, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm
						where thang_tldg_dt_dai = 202502 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_dnhm >0
					union all
						---dich vu ngoai ctr OR dvu khac VNPtt
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_ptm, doanhthu_kpi_nvptm dthu_kpi, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi = 202502 and (loaitb_id<>21 or ma_kh='GTGT rieng')
										and doanhthu_kpi_nvptm >0
					union all
					----Dthu nvho tro ban kenh ngoai
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_hotro, doanhthu_kpi_nvhotro, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi_hotro = 202502
										and tyle_am is null and tyle_hotro is null and (loaitb_id<>21 or ma_kh='GTGT rieng')
										and doanhthu_kpi_nvhotro >0
					union all
					---Dthu nvhotro PGP
						select cast(thang_ptm as number) thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_hotro, doanhthu_kpi_nvhotro, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm_pgp 
						where thang_tlkpi_hotro=202502 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_nvhotro >0
					union all
					---Dthu nv DAI
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_tt_dai, doanhthu_kpi_nvdai, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tldg_dt_dai = 202502 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_nvdai >0
					union all
					----Gia han VNPts
						select thang, ma_kh, ma_tb, 'VNPTS' dich_vu, 20 loaitb_id, 2 dichvuvt_id, ma_nv, doanhthu_dongia, 'vnpts_ptm_ghtt' nguon
						from ttkd_bsc.ghtt_vnpts 		--- Nguyen quan ly, chua co vb, toan noi mieng
						where thang=202502 and thang_giao is not null and ma_nv is not null
										--and ma_nv = 'VNP031710'
/*khong tinh quy doi dthu hien thu từ T2
					union all
					----GHTT toBHOL BHKV (Nhu Y)
						select a.THANG, null ma_gd, 'ghtt' ma_tb, 'Fiber', 58, 4, a.MA_NV, sum(TIEN) dthu_kpi, 'brcd_qd' nguon
						from ttkd_Bsc.tl_Giahan_Tratruoc a
									join ttkd_bsc.nhanvien nv on a.thang = nv.thang and a.ma_nv = nv.ma_nv
						where a.thang = 202502 and loai_tinh = 'DOANHTHU'
									--and nv.ma_vtcv not in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53')
									and a.ma_to in ('VNP0703004') --- to OBBH - PBHOL
						group by a.THANG, a.MA_NV
					----VB VNP hien huu 384
					union all
						select thang, loai_gd, ma_tb, 'VNP' || loaihinh_tb, decode(loaihinh_tb, 'TT', 21, 20), 2, ma_nv, dthu_kpi, 'vnp_qd' nguon
--						select *
						from ttkd_bsc.va_ct_bsc_vnphh
						where thang = 202502 
										and (ma_vtcv in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53') ---To BHOL BHKV
												or (ma_to in ('VNP0703004') and ma_vtcv in ('VNP-HNHCM_KDOL_17')) --- NV OBBH - To OBBH - PBHOL
												)
--										and ma_vtcv in ('VNP-HNHCM_KDOL_17') --- NV OBBH - To OBBH - PBHOL
*/
				)
--				where MANV_PTM in ('CTV071620','VNP027259')
				group by DICH_VU, LOAITB_ID, dichvuvt_id, manv_ptm, nguon
		;
	create index ttkd_bsc.temp_trasau_canhan_manv on ttkd_bsc.temp_trasau_canhan (ma_nv)
	;
	select *  from ttkd_bsc.tonghop_dthu_ptm where thang = 202502;
	insert into ttkd_bsc.tonghop_dthu_ptm
		select cast(202502 as number) thang, DICH_VU, LOAITB_ID, DICHVUVT_ID, MA_NV, DTHU_KPI 
		from ttkd_bsc.temp_trasau_canhan
		union all
		select thang_ptm, 'VNPTT', 21, 2, MANV_DNHM_KPI, sum(DTHU_DNHM_KPI) dthu_kpi
				from (
				select a.thang_ptm, MANV_DNHM_KPI, MATO_DNHM_KPI, MAPB_DNHM_KPI, round(DTHU_DNHM_KPI, 0) DTHU_DNHM_KPI
				from  ttkd_bsc.va_ct_bsc_ptm_vnptt a
				where a.thang_ptm = 202502 and DTHU_DNHM_KPI > 0 and MANV_DNHM_KPI is not null
				union all
				select a.thang_ptm, MANV_GOI_KPI, MATO_GOI_KPI, MAPB_GOI_KPI, round(DTHU_GOI_KPI, 0) DTHU_GOI_KPI
				from  ttkd_bsc.va_ct_bsc_ptm_vnptt a
				where a.thang_ptm = 202502 and DTHU_GOI_KPI > 0 and MANV_GOI_KPI is not null
				)
				group by thang_ptm, MANV_DNHM_KPI
	;
	commit;
-- to truong: thieu 0021_ts
	drop table ttkd_bsc.temp_totruong purge;
	
	select sum(dthu_kpi)/1000000 from ttkd_bsc.temp_totruong ---6790.62943335 --- 6796.93520735 --5102.73515635 --5538.69095635 --5539.25959435 --5539.31959435
	where ma_pb in ('VNP0701100', 'VNP0701200', 'VNP0701300', 'VNP0701400', 'VNP0701500','VNP0701600', 'VNP0701800', 'VNP0702100', 'VNP0702200', 'VNP0703000', 'VNP0702300', 'VNP0702400', 'VNP0702500');

	;
	select a.ma_nv, nv.ten_nv, ten_to, ten_pb, ten_vtcv, sum(dthu_kpi)
	from ttkd_bsc.temp_trasau_canhan a
					join ttkd_bsc.nhanvien nv on a.ma_nv = nv.ma_nv and nv.thang = 202502
			where nv.ma_to = 'VNP0701406'
	group by a.ma_nv, nv.ten_nv, ten_to, ten_pb, ten_vtcv
	;
	create table ttkd_bsc.temp_totruong as
			select ma_pb, ma_to, loaitb_id, dichvuvt_id, sum(doanhthu_kpi_to) dthu_kpi, nguon
			from(
					---- To NVPTM
					select ma_pb, ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_to * heso_hotro_nvptm as doanhthu_kpi_to, cast('vb353_ptm' as varchar(20)) nguon
					  from ttkd_bsc.ct_bsc_ptm a
					  where thang_tlkpi_to = 202502 and (loaitb_id<>21 or loaitb_id is null) 
									and doanhthu_kpi_to >0
				union all
					---- To NVHOTRO DIGISHOP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_to * heso_hotro_nvhotro as doanhthu_kpi_to, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
									join ttkd_bsc.nhanvien b on a.thang_tlkpi_to = b.thang and a.manv_hotro = b.ma_nv
					  where thang_tlkpi_to = 202502 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null 
									and doanhthu_kpi_to >0
									and nvl(vanban_id, 0) != 764  ---only T7, 8,9, thang sau xoa
				union all
					---To  Nvien Cot MANV_HOTRO PGP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_nvhotro, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
									join ttkd_bsc.nhanvien b on a.thang_tlkpi_hotro = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_hotro = 202502 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is not null and tyle_hotro is not null 
									and doanhthu_kpi_nvhotro >0
--				 ---MANV_DAI
					union all
						select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_to * heso_hotro_dai, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm a
										join ttkd_bsc.nhanvien b on a.thang_tlkpi_to = b.thang and a.manv_tt_dai = b.ma_nv
						where thang_tlkpi_to = 202502 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_to >0
									and nvl(vanban_id, 0) != 764  ---only T7, 8,9, thang sau xoa
				union all
					---To  Nvien Cot MANV_PTM cho dthu DNHM
					select ma_pb, ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm_phong * heso_hotro_nvptm as doanhthu_kpi_dnhm, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
					 where thang_tlkpi_dnhm_to = 202502 and (loaitb_id<>21 or loaitb_id is null)
									and doanhthu_kpi_dnhm_phong >0
				union all
					---To  Nvien Cot MANV_HOTRO cho dthu DNHM tren DIGISHOP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm_phong * heso_hotro_nvhotro as doanhthu_kpi_dnhm, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
								join ttkd_bsc.nhanvien b on a.thang_tlkpi_dnhm_to = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_dnhm_to = 202502 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null
									and doanhthu_kpi_dnhm_phong >0
									and nvl(vanban_id, 0) != 764 ---only T7,8,9, thang sau xoa
--				union all ---dthu DNHM PGP không có tinh Tổ trưởng
				union all
					select ma_pb, ma_to, ma_tb, 20 loaitb_id, 2 dichvuvt_id, doanhthu_dongia, 'vnpts_ptm_ghtt' nguon
					from ttkd_bsc.ghtt_vnpts		----a Nguyen quan ly
					  where thang=202502 and thang_giao is not null and ma_to is not null
--									and ma_nv = 'VNP031710'
/*khong tinh quy doi dthu hien thu từ T2				
				union all
					----GHTT toBHOL BHKV (Nhu Y)
						select a.ma_pb, a.ma_to, 'ghtt' ma_tb, 58, 4, sum(TIEN) dthu_kpi, 'brcd_qd' nguon
						from ttkd_Bsc.tl_Giahan_Tratruoc a
									join ttkd_bsc.nhanvien nv on a.thang = nv.thang and a.ma_nv = nv.ma_nv
						where a.thang = 202502 and loai_tinh = 'DOANHTHU'
--									and nv.ma_vtcv not in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53')
									and a.ma_to in ('VNP0703004') --- to OBBH - PBHOL
						group by a.ma_pb, a.ma_to
				----VB VNP hien huu 384 (Viet Anh)
					union all
						select ma_pb, ma_to, ma_tb, decode(loaihinh_tb, 'TT', 21, 20), 2, dthu_kpi, 'vnp_qd' nguon
--						select sum(dthu_kpi)
						from ttkd_bsc.va_ct_bsc_vnphh
						where thang = 202502 
									and (ma_vtcv in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53') ---To BHOL BHKV
												or (ma_to in ('VNP0703004') and ma_vtcv in ('VNP-HNHCM_KDOL_17')) --- NV OBBH - To OBBH - PBHOL
												)
--									and ma_vtcv in ('VNP-HNHCM_KDOL_17') --- NV OBBH - To OBBH - PBHOL
*/					  
						) group by ma_pb, ma_to, loaitb_id, dichvuvt_id, nguon
		;

						 
		
---- ldp phu trach: tinh theo MA_TO va NHOM dich vu
		drop table ttkd_bsc.x_temp_phong purge;
		create table ttkd_bsc.x_temp_phong as
			select ma_pb, ma_to, loaitb_id, dichvuvt_id, sum(doanhthu_kpi_phong) dthu_kpi, nguon
			from(
					---- To NVPTM
					select ma_pb, ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_phong * heso_hotro_nvptm as doanhthu_kpi_phong, cast('vb353_ptm' as varchar(20)) nguon
					  from ttkd_bsc.ct_bsc_ptm a
					  where thang_tlkpi_phong = 202502 and (loaitb_id<>21 or loaitb_id is null) 
									and doanhthu_kpi_phong >0
				union all
					---- To NVHOTRO DIGISHOP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_phong * heso_hotro_nvhotro as doanhthu_kpi_phong, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
									join ttkd_bsc.nhanvien b on a.thang_tlkpi_phong = b.thang and a.manv_hotro = b.ma_nv
					  where thang_tlkpi_phong = 202502 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null 
									and doanhthu_kpi_phong >0
									and nvl(vanban_id, 0) != 764 ---only T7,8,9, thang sau xoa
				union all
					---To  Nvien Cot MANV_HOTRO PGP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_phong * heso_hotro_nvhotro as doanhthu_kpi_phong, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
									join ttkd_bsc.nhanvien b on a.thang_tlkpi_phong = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_phong = 202502 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is not null and tyle_hotro is not null 
									and doanhthu_kpi_phong >0
--				 ---MANV_DAI
					union all
						select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_phong * heso_hotro_dai as doanhthu_kpi_phong, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm a
										join ttkd_bsc.nhanvien b on a.thang_tlkpi_phong = b.thang and a.manv_tt_dai = b.ma_nv
						where thang_tlkpi_phong = 202502 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_phong >0
									and nvl(vanban_id, 0) != 764  ---only T7, 8,9, thang sau xoa
				union all
					---************DNHM*****To  Nvien Cot MANV_PTM cho dthu DNHM
					select ma_pb, ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm_phong * heso_hotro_nvptm as doanhthu_kpi_dnhm_phong, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
					 where thang_tlkpi_dnhm_phong = 202502 and (loaitb_id<>21 or loaitb_id is null)
									and doanhthu_kpi_dnhm_phong >0
				union all
					---To  Nvien Cot MANV_HOTRO cho dthu DNHM tren DIGISHOP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm_phong * heso_hotro_nvhotro as doanhthu_kpi_dnhm_phong, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
								join ttkd_bsc.nhanvien b on a.thang_tlkpi_dnhm_phong = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_dnhm_phong = 202502 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null
									and doanhthu_kpi_dnhm_phong >0
									and nvl(vanban_id, 0) != 764 ---only T7, 8, 9, thang sau xoa
--				union all ---dthu DNHM PGP không có tinh Phó Giam đốc
				union all
					select ma_pb, ma_to, ma_tb, 20 loaitb_id, 2 dichvuvt_id, doanhthu_dongia, 'vnpts_ptm_ghtt' nguon
					from ttkd_bsc.ghtt_vnpts		----a Nguyen quan ly
					  where thang=202502 and thang_giao is not null and ma_to is not null
--									and ma_nv = 'VNP031710'
/*khong tinh quy doi dthu hien thu từ T2
				union all
					----GHTT toBHOL BHKV (Nhu Y)
						select a.ma_pb, a.ma_to, 'ghtt' ma_tb, 58, 4, sum(TIEN) dthu_kpi, 'brcd_qd' nguon
						from ttkd_Bsc.tl_Giahan_Tratruoc a
									join ttkd_bsc.nhanvien nv on a.thang = nv.thang and a.ma_nv = nv.ma_nv
						where a.thang = 202502 and loai_tinh = 'DOANHTHU'
--									and nv.ma_vtcv not in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53')
									and a.ma_to in ('VNP0703004') --- to OBBH - PBHOL
						group by a.ma_pb, a.ma_to
				----VB VNP hien huu 384
					union all
						select ma_pb, ma_to, ma_tb, decode(loaihinh_tb, 'TT', 21, 20), 2, dthu_kpi, 'vnp_qd' nguon
--						select *
						from ttkd_bsc.va_ct_bsc_vnphh
						where thang = 202502 
										and (ma_vtcv in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53') ---To BHOL BHKV
												or (ma_to in ('VNP0703004') and ma_vtcv in ('VNP-HNHCM_KDOL_17')) --- NV OBBH - To OBBH - PBHOL
												)
--										and ma_vtcv in ('VNP-HNHCM_KDOL_17') --- NV OBBH - To OBBH - PBHOL
*/
						) group by ma_pb, ma_to, loaitb_id, dichvuvt_id, nguon
		;
		----PTM
		select sum(DTHU_KPI)  from ttkd_bsc.temp_021_ldp1 
				where --dichvu in ('Mega+Fiber', 'MyTV') and 
--						NHOM_DICHVU  in ('Dichvu') and
						ma_pb in ('VNP0701100', 'VNP0701200', 'VNP0701300', 'VNP0701400', 'VNP0701500','VNP0701600', 'VNP0701800', 'VNP0702100', 'VNP0702200') 
						and ma_to  in (select ma_to from ttkd_bsc.nhanvien where thang = 202502)
						; 4635206943.2 --5032.68-- 5032.68066735 --5468.63646735 --5469.20510535 --5469.26510535
		select * from ttkd_bsc.temp_021_ldp1; 
		 select * from ttkd_bsc.temp_021_ldp where ma_pb in ('VNP0701100', 'VNP0701200', 'VNP0701300', 'VNP0701400', 'VNP0701500','VNP0701600', 'VNP0701800', 'VNP0702100', 'VNP0702200', 'VNP0703000', 'VNP0702300', 'VNP0702400', 'VNP0702500'); 
		 select sum(DTHU_KPI)/1000000 from ttkd_bsc.temp_021_ldp
		 where ma_pb in ('VNP0701100', 'VNP0701200', 'VNP0701300', 'VNP0701400', 'VNP0701500','VNP0701600', 'VNP0701800', 'VNP0702100', 'VNP0702200', 'VNP0703000', 'VNP0702300', 'VNP0702400', 'VNP0702500');  ---5052.26506235
		 
		 drop table  ttkd_bsc.temp_021_ldp1 purge
		 ;
		create table ttkd_bsc.temp_021_ldp1 as
				select a.ma_pb, a.ma_to, b.dv_cap1, b.dv_cap2
							, case when dv_cap1 in ('VNP tra sau', 'VNP tra truoc', 'Mega+Fiber', 'MyTV') then dv_cap1
										when dv_cap2 in ('Dich vu so doanh nghiep') then dv_cap2
									else 'Con lai' end PHUTRACH
							, case when dv_cap1 in ('VNP tra sau', 'VNP tra truoc', 'Mega+Fiber', 'MyTV') then 'Dichvu_cap1'
										when dv_cap2 in ('Dich vu so doanh nghiep') then 'Dichvu_cap2'
									else 'Dichvu' end NHOM
							, case when nguon = 'vnp_qd' then nguon else 'ptm' end nguon
							, sum(dthu_kpi) dthu_kpi
									from ttkd_bsc.x_temp_phong a
											left join ttkd_bsc.dm_loaihinh_hsqd b on a.loaitb_id = b.loaitb_id 
																and (dv_cap2 in ('Dich vu so doanh nghiep','Truyen so lieu', 'Internet truc tiep') or dv_cap1 in ('VNP tra sau', 'VNP tra truoc', 'Mega+Fiber', 'MyTV'))
											group by a.ma_pb, a.ma_to, b.dv_cap1, b.dv_cap2, nguon
			;
		select * from	ttkd_bsc.blkpi_dm_to_pgd pgd
							where pgd.thang=202502 
										and ma_pb in ('VNP0702300', 'VNP0702400', 'VNP0702500', 'VNP0703000')
										;
										select sum(DTHU_KPI)DTHU_KPI , PHUTRACH, NHOM from ttkd_bsc.temp_021_ldp group by PHUTRACH, NHOM
										;a.PHUTRACH = pgd.PHUTRACH and a.NHOM = pgd.NHOM
										
			drop table  ttkd_bsc.temp_021_ldp purge;
	create table ttkd_bsc.temp_021_ldp as	
--			select * from (
			with pgd as ( select distinct MA_PB, ten_pb, ten_to, MA_TO, MA_NV, THANG, TEN_NV, PHUTRACH, NHOM
									from ttkd_bsc.blkpi_dm_to_pgd pgd 
									where thang = 202502 and ma_kpi in ('HCM_DT_PTMOI_065') and pgd.PHUTRACH = 'To' and pgd.NHOM = 'Phutrach'
									)

						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									  join pgd
										on a.ma_to = pgd.ma_to and pgd.thang= 202502  -----All Phong theo To	
--				) --select *  from ttkd_bsc.temp_021_ldp 
--				where ma_nv in ('VNP017072', 'VNP017853')
			;
			---KIEM tra bang sau khi chay
					select sum(dthu_kpi) from
						(
						with pgd as ( select distinct MA_PB, MA_TO, MA_NV, THANG, dichvu, nhom_dichvu
												from ttkd_bsc.blkpi_dm_to_pgd pgd 
												where thang = 202502 and nhom_dichvu is not null
															)
									select a.*, pgd.ma_nv
													, case when exists (select 1 from pgd where ma_pb = a.ma_pb) then 1 else 0 end exit_ma_pb
													, case when exists (select 1 from pgd where ma_to = a.ma_to) then 1 else 0 end exit_ma_to
									from ttkd_bsc.temp_021_ldp1 a
												left join pgd on a.ma_to = pgd.ma_to and a.dichvu = pgd.dichvu and a.nhom_dichvu = pgd.nhom_dichvu and pgd.thang=202502
						) where exit_ma_pb + exit_ma_to = 2 and ma_nv is not null
		;
		commit;
		rollback;
		
		
		select * from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = 202502 ;
		
		select * from ttkd_bsc.bangluong_kpi a where ma_kpi in ('HCM_DT_PTMOI_021') and thang = 202502;
	
---*********--Tinh Tong dthu PTM (ngoai tru VNPtt)
		update ttkd_bsc.dinhmuc_giao_dthu_ptm 
					set KQTH = nvl(NHOMBRCD_KQTH, 0) + nvl(NHOMVINATS_KQTH, 0) + nvl(NHOMVINATT_KQTH, 0) + nvl(NHOMCNTT_KQTH, 0) + nvl(NHOMCONLAI_KQTH, 0) 
--						, canhan_thuchien = case when ma_vtcv in ('VNP-HNHCM_BHKV_27')
--																then nvl(NHOMBRCD_KQTH, 0) + nvl(NHOMVINATS_KQTH, 0) + nvl(NHOMVINATT_KQTH, 0) + nvl(NHOMCNTT_KQTH, 0) + nvl(NHOMCONLAI_KQTH, 0) 
--														else null end
--		select sum(KQTH) from ttkd_bsc.dinhmuc_giao_dthu_ptm --2 559 918 312 - 2 561 759 199
				where thang = 202502 
				;
		update ttkd_bsc.bangluong_kpi a 
					set THUCHIEN = (select round(nvl(KQTH, 0)/1000000, 3) from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = a.thang and ma_nv = a.ma_nv)
--				select * from  ttkd_bsc.bangluong_kpi a
				where ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_062', 'HCM_DT_PTMOI_065', 'HCM_DT_PTMOI_069') 
							and a.thang = 202502 
				;
		----update DTHU thau 2 NV Phong GP, hang thang lien he Phuoc gui
		update ttkd_bsc.bangluong_kpi
			set THUCHIEN = case when ma_nv = 'VNP027259' then THUCHIEN + 22000000/1000000
												when ma_nv = 'VNP017190' then THUCHIEN + 22000000/1000000
												end
--		select * from  ttkd_bsc.bangluong_kpi
		where thang = 202502 and ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_062', 'HCM_DT_PTMOI_065', 'HCM_DT_PTMOI_069') and ma_nv in ('VNP027259', 'VNP017190')
		;
		update ttkd_bsc.bangluong_kpi a 
			set NGAYCONG = 20
				where a.thang = 202502
		;
		---Khong GIAO Hong Duyen - BHOL
		update ttkd_bsc.bangluong_kpi a set thang = 2025029999
		where ma_kpi in ('HCM_DT_PTMOI_065') and a.thang = 202502
							and ma_nv  in ('VNP017072')
		;
		---Khong GIAO Duong Ba Vu - BHSG  các chi tiêu khác ngoài Dthu
		update ttkd_bsc.bangluong_kpi a set thang = 2025029999
		where ma_kpi not in ('HCM_DT_PTMOI_065') and a.thang = 202502
							and ma_nv  in ('VNP017014')
		;
		---
		update ttkd_bsc.bangluong_kpi a set CHITIEU_GIAO = 100, donvi_tinh = '%'
--			select * from ttkd_bsc.bangluong_kpi a 
				where ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_065', 'HCM_DT_PTMOI_069') and a.thang = 202502 --and ma_nv = 'CTV088841'
		;
		update ttkd_bsc.bangluong_kpi a set GIAO = 
																					case 
																							when ma_vtcv in ('VNP-HNHCM_GP_3') then 16		---fix so theo vb ap dung T11
																							when ma_vtcv in ('VNP-HNHCM_GP_3.4') then 16		---fix so theo vb ap dung T11
																								else	(select round(TONG_DTGIAO/1000000, 3) from ttkd_bsc.dinhmuc_giao_dthu_ptm 
																											where thang = a.thang and ma_nv = a.ma_nv 
																														--	and trunc(dateinput) = '28/07/2024'
																										)
																					end
--				select ma_nv, ten_nv, giao, thuchien, tyle_thuchien, mucdo_hoanthanh from ttkd_bsc.bangluong_kpi a
				where a.ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_065', 'HCM_DT_PTMOI_069') and a.thang = 202502-- and ma_pb != 'VNP0703000'
--				and ma_nv  in (select ma_nv from hocnq_ttkd.x_nv_tmp where XEPHANG_P1 is not null)
--				and ma_nv = 'VNP017694'
				
			;
		update ttkd_bsc.bangluong_kpi a set tytrong = case when ma_vtcv in ('VNP-HNHCM_GP_3') then 40	---fix so theo vb ap dung T8 NV PS PGP
																								when ma_vtcv in ('VNP-HNHCM_GP_3.4') then 80		---fix so theo vb ap dung T11
																								when ma_vtcv in ('VNP-HNHCM_KHDN_18') then 50	---fix so theo vb ap dung T2 NV AM chuyen ban BHDN
																								when ma_vtcv in ('VNP-HNHCM_KHDN_1') then 15		---fix so theo vb ap dung T2 GD BHDN T02/25
																								when ma_vtcv in ('VNP-HNHCM_KHDN_2') then 20		---fix so theo vb ap dung T2 PGD BHDN T02/25
																								when ma_vtcv in ('VNP-HNHCM_KHDN_4', 'VNP-HNHCM_KHDN_3', 'VNP-HNHCM_KHDN_23') then 20		---fix so theo vb ap dung T2 TL/AM/AM QLDL BHDN T02/25
																								
																								when ma_vtcv in ('VNP-HNHCM_KDOL_4', 'VNP-HNHCM_KDOL_5') then 55		---fix so theo vb ap dung T02/25 NV KDOL, TT KDOL
																								when ma_vtcv in ('VNP-HNHCM_KDOL_17') then 30		---fix so theo vb ap dung T02/25	NV OB BH
																								when ma_vtcv in ('VNP-HNHCM_KDOL_3.1', 'VNP-HNHCM_KDOL_17.1') then 20		---fix so theo vb ap dung T02/25Truong ca OB BH
																								
																								when ma_vtcv in ('VNP-HNHCM_BHKV_17', 'VNP-HNHCM_BHKV_15') then 60	---fix so theo vb ap dung TT/NV KD DDTT  T02/25
																								when ma_vtcv in ('VNP-HNHCM_BHKV_15.1') then 90	---fix so theo vb ap dung Tan Son Nhat  T02/25
																								when ma_vtcv in ('VNP-HNHCM_BHKV_6') then 50	---fix so theo vb ap dung T02/25 KDDB
																								when ma_vtcv in ('VNP-HNHCM_BHKV_42') then 50	---fix so theo vb ap dung T02/25  TT KDDB
																								when ma_vtcv in ('VNP-HNHCM_BHKV_1', 'VNP-HNHCM_BHKV_2.1') then 40	---fix so theo vb ap dung GD KV và PT KV  T02/25
																								
																								when ma_vtcv in ('VNP-HNHCM_BHKV_41') then 50	--thay doi theo thang AM BHKV T02/25
																								when ma_vtcv in ('VNP-HNHCM_BHKV_51') then 50	--thay doi theo thang TT CNTT KV T02/25
																								when ma_vtcv in ('VNP-HNHCM_BHKV_53') then 50	--thay doi theo thang NV OBBH - BHKV T02/25
																								when ma_vtcv in ('VNP-HNHCM_BHKV_52') then 50	--thay doi theo thang TT OBBH - BHKV T02/25
																								when ma_vtcv in ('VNP-HNHCM_BHKV_22') then 40	--thay doi theo thang GDV T02/25
																								when ma_vtcv in ('VNP-HNHCM_BHKV_28', 'VNP-HNHCM_BHKV_27') then 40	--thay doi theo thang CHT, CHT kGDV T02/25
																								----vi tri PGD theo vb va theo bang phan cong KPI nao, sheet nao trong vb
																								when ma_nv in ('VNP017740') and ma_vtcv in ('VNP-HNHCM_KDOL_2') then 25	-- T02/25 PGD OL Tuyet
																								when ma_nv in ('VNP020231', 'VNP017948') then 60		---sheet PGD PT DD TT T02/25
																								when ma_nv in ('VNP017014') then 100		---Duong Ba Vu PGD T02/25
																								when ma_vtcv in ('VNP-HNHCM_BHKV_2') then 50	---fix so theo vb ap dung PGD KV_các tổ KD - CH T2/25
--																								when ma_vtcv in ('VNP001757', 'VNP001724', 'VNP019529', 'VNP017948') then 100	---sheet PGD PT BH Ko PT BRCĐ
--																								when ma_nv in ('VNP016983') then 60 ---sheet PGD PT BH-CSKH-CH
--																								when ma_nv in ('VNP017496', 'VNP017585', 'VNP017947', 'VNP017729', 'VNP016659', 'VNP016898') then 70---sheet PGD PT BH-CSKH
																								
																						 end
--				select * from ttkd_bsc.bangluong_kpi a
				where a.ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_065', 'HCM_DT_PTMOI_069') and a.thang = 202502-- and ma_pb != 'VNP0703000'
			;
		update ttkd_bsc.bangluong_kpi a set TYLE_THUCHIEN = case when GIAO = 0 then null
																												when exists (select * from ttkd_bsc.bldg_danhmuc_vtcv_p1 where ma_vtcv not in ('VNP-HNHCM_BHKV_27') and thang = a.thang and ma_vtcv = a.ma_vtcv)		---chi ap dung nhan vien
																															and exists (select * from ttkd_bsc.dinhmuc_giao_dthu_ptm where XEPHANG_P1 in (1, 2, 3) and thang = a.thang and ma_nv = a.ma_nv) ---chi nhan vien có XEPHANG mức tối thiểu
																															and ROUND(THUCHIEN/GIAO*100, 2) >100		---tối đa 100%
																														THEN	100
																												else ROUND(THUCHIEN/GIAO*100, 2) 
																									end
--				select * from ttkd_bsc.bangluong_kpi a
				where ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_065', 'HCM_DT_PTMOI_069') and thang = 202502  
				
			;
		update ttkd_bsc.bangluong_kpi a set MUCDO_HOANTHANH = case 
																														--- case: khong danh gia BSC
																														when exists (select * from ttkd_bsc.nhanvien where thang = a.thang and ma_nv = a.ma_nv and tinh_bsc = 0)
																																	then 100
																														----P.PGP
																														when ma_vtcv in ('VNP-HNHCM_GP_3', 'VNP-HNHCM_GP_3.4')
																																	then case
																																				when TYLE_THUCHIEN < 30 then 0		-- 0%
																																				when TYLE_THUCHIEN >= 30 and TYLE_THUCHIEN <= 70
																																							then round(0.85 * TYLE_THUCHIEN, 2)			-- 85% *TLTH
																																				when TYLE_THUCHIEN > 70 and TYLE_THUCHIEN <= 100
																																							then round(1 * TYLE_THUCHIEN, 2)				-- 100% *TLTH
																																				when TYLE_THUCHIEN > 100
																																							then case when 100 + (1.2 * (TYLE_THUCHIEN - 100)) > 150 then 150 -- max 150%
																																												else round(100 + (1.2 * (TYLE_THUCHIEN - 100)), 2) end ---100% + 1.2 x (TLTH - 100%) 
																																				end
																														----P.BHOL Tuyet - PGD và các vị trí khác
																														when ma_nv in ('VNP017740') 
																																				or ma_vtcv in ('VNP-HNHCM_KDOL_2', 'VNP-HNHCM_KDOL_6', 'VNP-HNHCM_KDOL_17.1', 'VNP-HNHCM_KDOL_5')  ---BHOL: PGD, Tổ trưởng
																																				or ma_vtcv in ('VNP-HNHCM_KHDN_1', 'VNP-HNHCM_KHDN_2', 'VNP-HNHCM_KHDN_4')	--BHDN: GDP, PGD, Trưởng line
																																				or ma_vtcv in ('VNP-HNHCM_BHKV_1', 'VNP-HNHCM_BHKV_2.1', 'VNP-HNHCM_BHKV_2', 'VNP-HNHCM_BHKV_17'
																																												, 'VNP-HNHCM_BHKV_51', 'VNP-HNHCM_BHKV_42', 'VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_28', 'VNP-HNHCM_BHKV_27')	--BHKV
																																	then case
																																				when TYLE_THUCHIEN < 30 then 0		-- 0%
																																				when TYLE_THUCHIEN >= 30 and TYLE_THUCHIEN <= 70
																																							then round(0.85 * TYLE_THUCHIEN, 2)			-- 85% *TLTH
																																				when TYLE_THUCHIEN > 70 and TYLE_THUCHIEN <= 100
																																							then round(1 * TYLE_THUCHIEN, 2)				-- 100% *TLTH
																																				when TYLE_THUCHIEN > 100
																																							then case when 100 + (1.2 * (TYLE_THUCHIEN - 100)) > 200 then 200 -- max 200%
																																												else round(100 + (1.2 * (TYLE_THUCHIEN - 100)), 2) end ---100% + 1.2 x (TLTH - 100%) 
																																				end
																																	
																														
																														when ma_vtcv in ('VNP-HNHCM_KDOL_17', 'VNP-HNHCM_KDOL_4') 		--BHOL
																																	or ma_vtcv in ('VNP-HNHCM_KHDN_3', 'VNP-HNHCM_KHDN_23', 'VNP-HNHCM_KHDN_18')		--BHDN
																																	or ma_vtcv in ('VNP-HNHCM_BHKV_15', 'VNP-HNHCM_BHKV_15.1', 'VNP-HNHCM_BHKV_41', 'VNP-HNHCM_BHKV_6', 'VNP-HNHCM_BHKV_53', 'VNP-HNHCM_BHKV_22')		--BHKV
																																			then case when TYLE_THUCHIEN < 100 then 0  ---0%
																																								when TYLE_THUCHIEN >= 200 then 200  ---tối đa 200%
																																								else TYLE_THUCHIEN 
																																						end
																												end
--				select * from ttkd_bsc.bangluong_kpi a
				where ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_065', 'HCM_DT_PTMOI_069') and a.thang = 202502 --and ma_vtcv in ('VNP-HNHCM_GP_3', 'VNP-HNHCM_GP_3.4') 
--							and ma_nv = 'VNP019532'
--and ma_nv in ('VNP016950', 'VNP001757', 'VNP017203', 'VNP016659', 'HCM004899', 'VNP019529')
--and ma_nv in (select ma_nv from hocnq_ttkd.x_ds_lech_p1)
			;
		---Update ket qua chi AUCO --> a Ba Vu (dac thu)
		update ttkd_bsc.bangluong_kpi a 
						set (giao, thuchien, tyle_thuchien, mucdo_hoanthanh) = (select giao, thuchien, tyle_thuchien, mucdo_hoanthanh 
																														from ttkd_bsc.bangluong_kpi where ma_nv = 'VNP016730' and ma_kpi = a.ma_kpi and a.thang = thang)
								, tytrong = 100
		where ma_nv = 'VNP017014' and ma_kpi in ('HCM_DT_PTMOI_065') and thang = 202502
		;
		select * from ttkd_bsc.bangluong_kpi where ma_kpi in ('HCM_DT_PTMOI_065') and thang = 202502
		and ma_nv in ('VNP017014', 'VNP016730')
--		where ma_nv = 'CTV028802'
		;
commit;
rollback;
		
;
		----tinh heso doanhthu de tinh dongia
		select * from ttkd_bsc.bldg_danhmuc_vtcv_p1 where thang = 202502;
		---Heso dthu CHT kGDV lay theo muc chuan trong bang ttkd_bsc.bldg_danhmuc_vtcv_p1
		----Update Heso dthu tu 2 table ttkd_bsc.dinhmuc_giao_ptm va TLTH chi tieu 1 KHDN
			---Ap dung vb 323 dv BHKV
		---Update dthu thuc hien ca nhan CHT KGV
		MERGE INTO ttkd_bsc.dinhmuc_giao_dthu_ptm a
		USING (select thang, ma_to, sum(kqth) kqth from ttkd_bsc.dinhmuc_giao_dthu_ptm where LOAI_KPI = 'KPI_NV' group by thang, ma_to) b
		ON (a.thang = b.thang and a.ma_to = b.ma_to)
		WHEN MATCHED THEN
			UPDATE SET a.canhan_thuchien = a.kqth - nvl(b.kqth, 0)
--			select * from ttkd_bsc.dinhmuc_giao_dthu_ptm a
		WHERE a.thang = 202502 and loai_kpi = 'KPI_CHT_GDV' 
		;
				--CH chi co 1 CHT_kGDV
		UPDATE  ttkd_bsc.dinhmuc_giao_dthu_ptm a set a.canhan_thuchien = a.kqth
		WHERE a.thang = 202502 and loai_kpi = 'KPI_CHT_GDV' and canhan_thuchien is null
		;
	
			update ttkd_bsc.dinhmuc_giao_dthu_ptm a
							set HESO_QD_DT_PTM = case when exists (select * from ttkd_bsc.nhanvien where tinh_bsc = 0 and thang = a.thang and ma_nv = a.ma_nv) then 0.8
																				when ma_vtcv in ('VNP-HNHCM_BHKV_27') then 1			--do CHT kGV không Xep Bac nên gán hs nâng suất = 1
																				when XEPHANG_P1 = 5 and KQTH >= dinhmuc_1 then 1.1 --1
																				when XEPHANG_P1 = 5 and KQTH >= dinhmuc_2 and KQTH < dinhmuc_1 then 1 --2
																				when XEPHANG_P1 = 5 and KQTH >= dinhmuc_3 and KQTH < dinhmuc_2 then 0.95 --3
																				when XEPHANG_P1 = 5 and KQTH < dinhmuc_3 then 0.95 --4
																				when XEPHANG_P1 = 4 and KQTH >= dinhmuc_1 then 1 --5
																				when XEPHANG_P1 = 4 and KQTH >= dinhmuc_2 and KQTH < dinhmuc_1 then 1 --6
																				when XEPHANG_P1 = 4 and KQTH >= dinhmuc_3 and KQTH < dinhmuc_2 then 0.95 --7
																				when XEPHANG_P1 = 4 and KQTH < dinhmuc_3 then 0.9 --8
																				
																				when XEPHANG_P1 = 3 and KQTH >= dinhmuc_1 then 0.95 --9
																				when XEPHANG_P1 = 3 and KQTH >= dinhmuc_2 and KQTH < dinhmuc_1 then 0.95 --10
																				when XEPHANG_P1 = 3 and KQTH >= dinhmuc_3 and KQTH < dinhmuc_2 then 0.9 --11
																				when XEPHANG_P1 = 3 and KQTH < dinhmuc_3 then 0.85 --12
																				
																				when XEPHANG_P1 = 2 and KQTH >= dinhmuc_1 then 0.9 --13
																				when XEPHANG_P1 = 2 and KQTH >= dinhmuc_2 and KQTH < dinhmuc_1 then 0.9 --14
																				when XEPHANG_P1 = 2 and KQTH >= dinhmuc_3 and KQTH < dinhmuc_2 then 0.85 --15
																				when XEPHANG_P1 = 2 and KQTH < dinhmuc_3 then 0.8 --16
																				
																				when XEPHANG_P1 = 1 and KQTH >= dinhmuc_1 then 0.8 --17
																				when XEPHANG_P1 = 1 and KQTH >= dinhmuc_2 and KQTH < dinhmuc_1 then 0.8 --18
																				when XEPHANG_P1 = 1 and KQTH >= dinhmuc_3 and KQTH < dinhmuc_2 then 0.8 --19
																				when XEPHANG_P1 = 1 and KQTH < dinhmuc_3 then 0.8 --20
																				else null
																		end
--					select * from ttkd_bsc.dinhmuc_giao_dthu_ptm a
					where thang = 202502
								and a.ma_vtcv in (select MA_VTCV
													from ttkd_bsc.bldg_danhmuc_vtcv_p1 where thang = a.thang)
								and a.ma_vtcv not in ('VNP-HNHCM_KHDN_3') --and ma_nv  in (select ma_nv from hocnq_ttkd.x_nv_tmp where XEPHANG_P1 is not null)

;
		---Ap dung vb 292 dv BHDN eO 552546 --> ap dung AM ban cham BHDN
		update ttkd_bsc.dinhmuc_giao_dthu_ptm a 
						set HESO_QD_DT_PTM = (select 
																		case 
																				when TYLE_THUCHIEN is null then 0.8
																				when TYLE_THUCHIEN < 80 then 0.8
																				when TYLE_THUCHIEN >= 80 and TYLE_THUCHIEN < 90 then 0.85
																				when TYLE_THUCHIEN >= 90 and TYLE_THUCHIEN < 95 then 0.9
																				when TYLE_THUCHIEN >= 95 and TYLE_THUCHIEN < 98 then 0.95
																				when TYLE_THUCHIEN >= 98 and TYLE_THUCHIEN < 100 then 1
																				when TYLE_THUCHIEN >= 100 then 1.1 else null end heso_qd_dthu
																from ttkd_bsc.bangluong_kpi 
																where ma_kpi in ('HCM_DT_LUYKE_002') and thang = a.thang and ma_nv = a.ma_nv
															)
--			select * from ttkd_bsc.dinhmuc_giao_dthu_ptm a 															
			where thang = 202502
						and ma_vtcv  in ('VNP-HNHCM_KHDN_3') 
						and ma_pb in (
											'VNP0702300',
											'VNP0702400',
											'VNP0702500'
											) 
			;
	
	rollback;
	commit;
------------------------------------------------------------------------------------------------

select * from ttkd_bsc.bldg_danhmuc_vtcv_p1 where thang = 202502;
select *
		from admin.v_hs_thuebao a
					join admin.v_file_hs b on a.FILE_ID = b.FILE_ID
					;

		
		


