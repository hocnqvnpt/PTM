alter table ttkd_bsc.bangluong_kpi read only;
alter table ttkd_bsc.bangluong_kpi read write;
/*
create table bangluong_kpi_202501_l6 as select * from bangluong_kpi_202501;  


ktra sau khi add du lieu thang do Xuan Vinh cung cap
select * from ttkd_bsc.dinhmuc_dthu_ptm where  thang=202501 ;
update ttkd_bsc.dinhmuc_dthu_ptm set  thang=202501 where thang is null;
update ttkd_bsc.dinhmuc_dthu_ptm set dt_giao_bsc='' where thang=202501 and dt_giao_bsc=0;
*/
create table ttkd_bsc.bangluong_kpi_202501_dot2 as select * from ttkd_bsc.bangluong_kpi_202501;  
create table ttkd_bsc.bangluong_kpi_20250122_dot3 as select * from ttkd_bsc.bangluong_kpi where thang = 202501;  
select * from ttkd_bsc.bangluong_kpi_dot2_20250120 where thang = 202501;  

select distinct a.*, b.*, c.ten_vtcv 
		from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien c
        where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv and c.thang= 202501
                    and a.thang = b.thang and a.thang = c.thang
                    and a.ma_kpi='HCM_DT_PTMOI_021' ;

--      select * from ttkd_bsc.temp_trasau_canhan     
          select * from ttkd_bsc.temp_trasau_canhan;
		drop table ttkd_bsc.temp_trasau_canhan purge;
		desc ttkd_bsc.ghtt_vnpts;
		;3329.5529125 -- 3377.4606315 - 8807.6642255 -- 3402.5087115 - 8839.3039055
		select sum(dthu_kpi)/1000000
		from ttkd_bsc.temp_trasau_canhan a
							join ttkd_bsc.nhanvien nv on nv.thang = 202501 and a.ma_nv = nv.ma_nv;
		where nv.ma_pb in ('VNP0701100', 'VNP0701200', 'VNP0701300', 'VNP0701400', 'VNP0701500','VNP0701600', 'VNP0701800', 'VNP0702100', 'VNP0702200', 'VNP0703000')
							 ;
		----Tat ca dich vu, ngoai tru VNPtt
		create table ttkd_bsc.temp_trasau_canhan as
--		insert into ttkd_bsc.temp_trasau_canhan --(MANV_PTM, DTHU_KPI)
				select DICH_VU, LOAITB_ID, dichvuvt_id, cast(manv_ptm as varchar(20)) ma_nv,  sum(dthu_kpi) dthu_kpi, nguon
				from (
					
						---DNHM cho cot NVPTM
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_ptm, doanhthu_kpi_dnhm * heso_hotro_nvptm dthu_kpi, cast('vb353_ptm' as varchar(20)) nguon
						from ttkd_bsc.ct_bsc_ptm
						where thang_tlkpi_dnhm = 202501 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_dnhm >0
					union all
					---DNHM cho cot NVHOTRO kenh ngoai
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_hotro, doanhthu_kpi_dnhm * heso_hotro_nvhotro doanhthu_kpi_dnhm, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm
						where thang_tlkpi_hotro = 202501 
									and tyle_am is null and tyle_hotro is null and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_dnhm >0
					union all
					---DNHM cho cot DAI
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_tt_dai, doanhthu_kpi_dnhm * heso_hotro_dai doanhthu_kpi_dnhm, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm
						where thang_tldg_dt_dai = 202501 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_dnhm >0
					union all
						---dich vu ngoai ctr OR dvu khac VNPtt
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_ptm, doanhthu_kpi_nvptm dthu_kpi, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi = 202501 and (loaitb_id<>21 or ma_kh='GTGT rieng')
										and doanhthu_kpi_nvptm >0
					union all
					----Dthu nvho tro ban kenh ngoai
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_hotro, doanhthu_kpi_nvhotro, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi_hotro = 202501
										and tyle_am is null and tyle_hotro is null and (loaitb_id<>21 or ma_kh='GTGT rieng')
										and doanhthu_kpi_nvhotro >0
					union all
					---Dthu nvhotro PGP
						select cast(thang_ptm as number) thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_hotro, doanhthu_kpi_nvhotro, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm_pgp 
						where thang_tlkpi_hotro=202501 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_nvhotro >0
					union all
					---Dthu nv DAI
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_tt_dai, doanhthu_kpi_nvdai, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tldg_dt_dai = 202501 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_nvdai >0
					union all
					----Gia han VNPts
						select thang, ma_kh, ma_tb, 'VNPTS' dich_vu, 20 loaitb_id, 2 dichvuvt_id, ma_nv, doanhthu_dongia, 'vnpts_ptm_ghtt' nguon
						from ttkd_bsc.ghtt_vnpts 		--- Nguyen quan ly, chua co vb, toan noi mieng
						where thang=202501 and thang_giao is not null and ma_nv is not null
										--and ma_nv = 'VNP031710'
					union all
					----GHTT toBHOL BHKV (Nhu Y)
						select a.THANG, null ma_gd, 'ghtt' ma_tb, 'Fiber', 58, 4, a.MA_NV, sum(TIEN) dthu_kpi, 'brcd_qd' nguon
						from ttkd_Bsc.tl_Giahan_Tratruoc a
									join ttkd_bsc.nhanvien nv on a.thang = nv.thang and a.ma_nv = nv.ma_nv
						where a.thang = 202501 and loai_tinh = 'DOANHTHU'
									--and nv.ma_vtcv not in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53')
									and a.ma_to in ('VNP0703004') --- to OBBH - PBHOL
						group by a.THANG, a.MA_NV
					----VB VNP hien huu 384
					union all
						select thang, loai_gd, ma_tb, 'VNP' || loaihinh_tb, decode(loaihinh_tb, 'TT', 21, 20), 2, ma_nv, dthu_kpi, 'vnp_qd' nguon
--						select *
						from ttkd_bsc.va_ct_bsc_vnphh
						where thang = 202501 
										and (ma_vtcv in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53') ---To BHOL BHKV
												or (ma_to in ('VNP0703004') and ma_vtcv in ('VNP-HNHCM_KDOL_17')) --- NV OBBH - To OBBH - PBHOL
												)
--										and ma_vtcv in ('VNP-HNHCM_KDOL_17') --- NV OBBH - To OBBH - PBHOL
				)
--				where MANV_PTM in ('CTV071620','VNP027259')
				group by DICH_VU, LOAITB_ID, dichvuvt_id, manv_ptm, nguon
		;
	create index ttkd_bsc.temp_trasau_canhan_manv on ttkd_bsc.temp_trasau_canhan (ma_nv)
	;
	delete from ttkd_bsc.tonghop_dthu_ptm where thang = 202501;
	insert into ttkd_bsc.tonghop_dthu_ptm
		select cast(202501 as number) thang, DICH_VU, LOAITB_ID, DICHVUVT_ID, MA_NV, DTHU_KPI 
		from ttkd_bsc.temp_trasau_canhan
		union all
		select thang_ptm, 'VNPTT', 21, 2, MANV_DNHM_KPI, sum(DTHU_DNHM_KPI) dthu_kpi
				from (
				select a.thang_ptm, MANV_DNHM_KPI, MATO_DNHM_KPI, MAPB_DNHM_KPI, round(DTHU_DNHM_KPI, 0) DTHU_DNHM_KPI
				from  ttkd_bsc.va_ct_bsc_ptm_vnptt a
				where a.thang_ptm = 202501 and DTHU_DNHM_KPI > 0 and MANV_DNHM_KPI is not null
				union all
				select a.thang_ptm, MANV_GOI_KPI, MATO_GOI_KPI, MAPB_GOI_KPI, round(DTHU_GOI_KPI, 0) DTHU_GOI_KPI
				from  ttkd_bsc.va_ct_bsc_ptm_vnptt a
				where a.thang_ptm = 202501 and DTHU_GOI_KPI > 0 and MANV_GOI_KPI is not null
				)
				group by thang_ptm, MANV_DNHM_KPI
	;
	commit;
-- to truong: thieu 0021_ts
	drop table ttkd_bsc.temp_totruong purge;
	
	select sum(dthu_kpi)/1000000 from ttkd_bsc.temp_totruong ---3329.5529125 --- 3378.4736737
	where ma_pb in ('VNP0701100', 'VNP0701200', 'VNP0701300', 'VNP0701400', 'VNP0701500','VNP0701600', 'VNP0701800', 'VNP0702100', 'VNP0702200', 'VNP0703000');
	select * from ttkd_bsc.temp_totruong where ma_to = 'VNP0701210'; 9 984 154 803.9 (10 999 250 012.15 )
	union all 
	;
	select a.ma_nv, nv.ten_nv, ten_to, ten_pb, ten_vtcv, sum(dthu_kpi)
	from ttkd_bsc.temp_trasau_canhan a
					join ttkd_bsc.nhanvien nv on a.ma_nv = nv.ma_nv and nv.thang = 202501
			where nv.ma_to = 'VNP0701406'
	group by a.ma_nv, nv.ten_nv, ten_to, ten_pb, ten_vtcv
	;
	create table ttkd_bsc.temp_totruong as
			select ma_pb, ma_to, loaitb_id, dichvuvt_id, sum(doanhthu_kpi_to) dthu_kpi, nguon
			from(
					---- To NVPTM
					select ma_pb, ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_to * heso_hotro_nvptm as doanhthu_kpi_to, cast('vb353_ptm' as varchar(20)) nguon
					  from ttkd_bsc.ct_bsc_ptm a
					  where thang_tlkpi_to = 202501 and (loaitb_id<>21 or loaitb_id is null) 
									and doanhthu_kpi_to >0
				union all
					---- To NVHOTRO DIGISHOP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_to * heso_hotro_nvhotro as doanhthu_kpi_to, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
									join ttkd_bsc.nhanvien b on a.thang_tlkpi_to = b.thang and a.manv_hotro = b.ma_nv
					  where thang_tlkpi_to = 202501 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null 
									and doanhthu_kpi_to >0
									and nvl(vanban_id, 0) != 764  ---only T7, 8,9, thang sau xoa
				union all
					---To  Nvien Cot MANV_HOTRO PGP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_nvhotro, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
									join ttkd_bsc.nhanvien b on a.thang_tlkpi_hotro = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_hotro = 202501 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is not null and tyle_hotro is not null 
									and doanhthu_kpi_nvhotro >0
--				 ---MANV_DAI
					union all
						select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_to * heso_hotro_dai, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm a
										join ttkd_bsc.nhanvien b on a.thang_tlkpi_to = b.thang and a.manv_tt_dai = b.ma_nv
						where thang_tlkpi_to = 202501 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_to >0
									and nvl(vanban_id, 0) != 764  ---only T7, 8,9, thang sau xoa
				union all
					---To  Nvien Cot MANV_PTM cho dthu DNHM
					select ma_pb, ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm_phong * heso_hotro_nvptm as doanhthu_kpi_dnhm, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
					 where thang_tlkpi_dnhm_to = 202501 and (loaitb_id<>21 or loaitb_id is null)
									and doanhthu_kpi_dnhm_phong >0
				union all
					---To  Nvien Cot MANV_HOTRO cho dthu DNHM tren DIGISHOP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm_phong * heso_hotro_nvhotro as doanhthu_kpi_dnhm, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
								join ttkd_bsc.nhanvien b on a.thang_tlkpi_dnhm_to = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_dnhm_to = 202501 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null
									and doanhthu_kpi_dnhm_phong >0
									and nvl(vanban_id, 0) != 764 ---only T7,8,9, thang sau xoa
--				union all ---dthu DNHM PGP không có tinh Tổ trưởng
				union all
					select ma_pb, ma_to, ma_tb, 20 loaitb_id, 2 dichvuvt_id, doanhthu_dongia, 'vnpts_ptm_ghtt' nguon
					from ttkd_bsc.ghtt_vnpts		----a Nguyen quan ly
					  where thang=202501 and thang_giao is not null and ma_to is not null
--									and ma_nv = 'VNP031710'
				
				union all
					----GHTT toBHOL BHKV (Nhu Y)
						select a.ma_pb, a.ma_to, 'ghtt' ma_tb, 58, 4, sum(TIEN) dthu_kpi, 'brcd_qd' nguon
						from ttkd_Bsc.tl_Giahan_Tratruoc a
									join ttkd_bsc.nhanvien nv on a.thang = nv.thang and a.ma_nv = nv.ma_nv
						where a.thang = 202501 and loai_tinh = 'DOANHTHU'
--									and nv.ma_vtcv not in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53')
									and a.ma_to in ('VNP0703004') --- to OBBH - PBHOL
						group by a.ma_pb, a.ma_to
				----VB VNP hien huu 384 (Viet Anh)
					union all
						select ma_pb, ma_to, ma_tb, decode(loaihinh_tb, 'TT', 21, 20), 2, dthu_kpi, 'vnp_qd' nguon
--						select sum(dthu_kpi)
						from ttkd_bsc.va_ct_bsc_vnphh
						where thang = 202501 
									and (ma_vtcv in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53') ---To BHOL BHKV
												or (ma_to in ('VNP0703004') and ma_vtcv in ('VNP-HNHCM_KDOL_17')) --- NV OBBH - To OBBH - PBHOL
												)
--									and ma_vtcv in ('VNP-HNHCM_KDOL_17') --- NV OBBH - To OBBH - PBHOL
					  
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
					  where thang_tlkpi_phong = 202501 and (loaitb_id<>21 or loaitb_id is null) 
									and doanhthu_kpi_phong >0
				union all
					---- To NVHOTRO DIGISHOP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_phong * heso_hotro_nvhotro as doanhthu_kpi_phong, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
									join ttkd_bsc.nhanvien b on a.thang_tlkpi_phong = b.thang and a.manv_hotro = b.ma_nv
					  where thang_tlkpi_phong = 202501 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null 
									and doanhthu_kpi_phong >0
									and nvl(vanban_id, 0) != 764 ---only T7,8,9, thang sau xoa
				union all
					---To  Nvien Cot MANV_HOTRO PGP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_phong * heso_hotro_nvhotro as doanhthu_kpi_phong, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
									join ttkd_bsc.nhanvien b on a.thang_tlkpi_phong = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_phong = 202501 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is not null and tyle_hotro is not null 
									and doanhthu_kpi_phong >0
--				 ---MANV_DAI
					union all
						select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_phong * heso_hotro_dai as doanhthu_kpi_phong, 'vb353_ptm' nguon
						from ttkd_bsc.ct_bsc_ptm a
										join ttkd_bsc.nhanvien b on a.thang_tlkpi_phong = b.thang and a.manv_tt_dai = b.ma_nv
						where thang_tlkpi_phong = 202501 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_phong >0
									and nvl(vanban_id, 0) != 764  ---only T7, 8,9, thang sau xoa
				union all
					---************DNHM*****To  Nvien Cot MANV_PTM cho dthu DNHM
					select ma_pb, ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm_phong * heso_hotro_nvptm as doanhthu_kpi_dnhm_phong, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
					 where thang_tlkpi_dnhm_phong = 202501 and (loaitb_id<>21 or loaitb_id is null)
									and doanhthu_kpi_dnhm_phong >0
				union all
					---To  Nvien Cot MANV_HOTRO cho dthu DNHM tren DIGISHOP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm_phong * heso_hotro_nvhotro as doanhthu_kpi_dnhm_phong, 'vb353_ptm' nguon
					  from ttkd_bsc.ct_bsc_ptm a
								join ttkd_bsc.nhanvien b on a.thang_tlkpi_dnhm_phong = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_dnhm_phong = 202501 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null
									and doanhthu_kpi_dnhm_phong >0
									and nvl(vanban_id, 0) != 764 ---only T7, 8, 9, thang sau xoa
--				union all ---dthu DNHM PGP không có tinh Phó Giam đốc
				union all
					select ma_pb, ma_to, ma_tb, 20 loaitb_id, 2 dichvuvt_id, doanhthu_dongia, 'vnpts_ptm_ghtt' nguon
					from ttkd_bsc.ghtt_vnpts		----a Nguyen quan ly
					  where thang=202501 and thang_giao is not null and ma_to is not null
--									and ma_nv = 'VNP031710'
				
				union all
					----GHTT toBHOL BHKV (Nhu Y)
						select a.ma_pb, a.ma_to, 'ghtt' ma_tb, 58, 4, sum(TIEN) dthu_kpi, 'brcd_qd' nguon
						from ttkd_Bsc.tl_Giahan_Tratruoc a
									join ttkd_bsc.nhanvien nv on a.thang = nv.thang and a.ma_nv = nv.ma_nv
						where a.thang = 202501 and loai_tinh = 'DOANHTHU'
--									and nv.ma_vtcv not in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53')
									and a.ma_to in ('VNP0703004') --- to OBBH - PBHOL
						group by a.ma_pb, a.ma_to
				----VB VNP hien huu 384
					union all
						select ma_pb, ma_to, ma_tb, decode(loaihinh_tb, 'TT', 21, 20), 2, dthu_kpi, 'vnp_qd' nguon
--						select *
						from ttkd_bsc.va_ct_bsc_vnphh
						where thang = 202501 
										and (ma_vtcv in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53') ---To BHOL BHKV
												or (ma_to in ('VNP0703004') and ma_vtcv in ('VNP-HNHCM_KDOL_17')) --- NV OBBH - To OBBH - PBHOL
												)
--										and ma_vtcv in ('VNP-HNHCM_KDOL_17') --- NV OBBH - To OBBH - PBHOL

						) group by ma_pb, ma_to, loaitb_id, dichvuvt_id, nguon
		;
		----PTM
		select sum(DTHU_KPI)  from ttkd_bsc.temp_021_ldp1 
				where --dichvu in ('Mega+Fiber', 'MyTV') and 
--						NHOM_DICHVU  in ('Dichvu') and
						ma_pb in ('VNP0701100', 'VNP0701200', 'VNP0701300', 'VNP0701400', 'VNP0701500','VNP0701600', 'VNP0701800', 'VNP0702100', 'VNP0702200') 
						and ma_to  in (select ma_to from ttkd_bsc.nhanvien where thang = 202501)
						; 4635206943.2
		select * from ttkd_bsc.temp_021_ldp1; where ma_pb in ('VNP0701100', 'VNP0701200', 'VNP0701300', 'VNP0701400', 'VNP0701500','VNP0701600', 'VNP0701800', 'VNP0702100', 'VNP0702200');
		 select * from ttkd_bsc.x_temp_phong; where ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500'); 9379.411
		 select sum(DTHU_KPI)/1000000 from ttkd_bsc.x_temp_phong 
		 where ma_pb in ('VNP0701100', 'VNP0701200', 'VNP0701300', 'VNP0701400', 'VNP0701500','VNP0701600', 'VNP0701800', 'VNP0702100', 'VNP0702200');  ---3112.8066864
		 
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
							where pgd.thang=202501 
										and ma_pb in ('VNP0702300', 'VNP0702400', 'VNP0702500', 'VNP0703000')
										;
										select sum(DTHU_KPI)DTHU_KPI , PHUTRACH, NHOM from ttkd_bsc.temp_021_ldp group by PHUTRACH, NHOM
										;a.PHUTRACH = pgd.PHUTRACH and a.NHOM = pgd.NHOM
										
			drop table  ttkd_bsc.temp_021_ldp purge;
	create table ttkd_bsc.temp_021_ldp as	
--			select * from (
			with pgd as ( select distinct MA_PB, ten_to, MA_TO, MA_NV, THANG, dichvu, nhom_dichvu, TEN_NV, PHUTRACH, NHOM
									from ttkd_bsc.blkpi_dm_to_pgd pgd 
									where thang = 202501 and ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500') and ma_kpi in ('HCM_DT_PTMOI_021', 'HCM_DT_PTMOI_062')
									)

						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									  join pgd
										on a.ma_to = pgd.ma_to and a.PHUTRACH = pgd.PHUTRACH and a.NHOM = pgd.NHOM and pgd.thang=202501 
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500', 'VNP0703000')		---BHKV theo To va Dich vu VNP tra sau
									and a.PHUTRACH in ('VNP tra sau', 'VNP tra truoc') --and a.nguon = 'ptm'
						union all
						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									 join pgd
										on a.ma_to = pgd.ma_to and a.PHUTRACH = pgd.PHUTRACH and a.NHOM = pgd.NHOM  and pgd.thang=202501 
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500', 'VNP0703000')		---BHKV theo To va Dich vu 'Mega, Fiber', 'MyTV'
										and a.PHUTRACH in ('Mega+Fiber', 'MyTV') and a.nguon = 'ptm'
						union all
						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									  join pgd
										on a.ma_to = pgd.ma_to and a.PHUTRACH = pgd.PHUTRACH and a.NHOM = pgd.NHOM  and pgd.thang=202501 
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500', 'VNP0703000')		---BHKV theo To va Dich vu Dich vu so doanh nghiep
										and a.PHUTRACH in ('Dich vu so doanh nghiep') and a.nguon = 'ptm'
						union all
						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									  join pgd on a.ma_to = pgd.ma_to and a.PHUTRACH = pgd.PHUTRACH and a.NHOM = pgd.NHOM  and pgd.thang=202501 
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500', 'VNP0703000')		---BHKV theo To va Dich vu TSL, INT, con lai
									and a.NHOM in ('Dichvu') and a.nguon = 'ptm'
						union all
						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									  join pgd on a.ma_to = pgd.ma_to and pgd.thang=202501 
						where a.ma_pb in ('VNP0703000')		---PBHOL theo To										
--				) --select *  from ttkd_bsc.temp_021_ldp 
--				where ma_nv in ('VNP017072', 'VNP017853')
			;
			---KIEM tra bang sau khi chay
					select sum(dthu_kpi) from
						(
						with pgd as ( select distinct MA_PB, MA_TO, MA_NV, THANG, dichvu, nhom_dichvu
												from ttkd_bsc.blkpi_dm_to_pgd pgd 
												where thang = 202501 and nhom_dichvu is not null
															)
									select a.*, pgd.ma_nv
													, case when exists (select 1 from pgd where ma_pb = a.ma_pb) then 1 else 0 end exit_ma_pb
													, case when exists (select 1 from pgd where ma_to = a.ma_to) then 1 else 0 end exit_ma_to
									from ttkd_bsc.temp_021_ldp1 a
												left join pgd on a.ma_to = pgd.ma_to and a.dichvu = pgd.dichvu and a.nhom_dichvu = pgd.nhom_dichvu and pgd.thang=202501
						) where exit_ma_pb + exit_ma_to = 2 and ma_nv is not null
		;
		commit;
		rollback;
		
		select * from ttkd_bsc.blkpi_danhmuc;
		select * from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = 202501 ;
		select ma_nv, ten_vtcv, hcm_dt_ptmoi_021_vnptt, hcm_dt_ptmoi_021_vnpts, hcm_dt_ptmoi_021_cdbr_mytv, hcm_dt_ptmoi_021_cntt, HCM_DT_PTMOI_021 from ttkd_bsc.bangluong_kpi_202501 a;
		select * from ttkd_bsc.bangluong_kpi a where ma_kpi in ('HCM_DT_PTMOI_021') and thang = 202501;
		select * from ttkdhcm_ktnv.ID430_TTDANGKY_CHOTTHANG where thang = 202501 and manv_hrm = 'VNP016902';
---*********--Tinh Tong dthu PTM (ngoai tru VNPtt)
		update ttkd_bsc.dinhmuc_giao_dthu_ptm 
					set KQTH = nvl(NHOMBRCD_KQTH, 0) + nvl(NHOMVINATS_KQTH, 0) + nvl(NHOMVINATT_KQTH, 0) + nvl(NHOMCNTT_KQTH, 0) + nvl(NHOMCONLAI_KQTH, 0) + nvl(NHOMVNPQD_KQTH, 0)
--						, canhan_thuchien = case when ma_vtcv in ('VNP-HNHCM_BHKV_27')
--																then nvl(NHOMBRCD_KQTH, 0) + nvl(NHOMVINATS_KQTH, 0) + nvl(NHOMVINATT_KQTH, 0) + nvl(NHOMCNTT_KQTH, 0) + nvl(NHOMCONLAI_KQTH, 0) 
--														else null end
--		select KQTH, canhan_thuchien from ttkd_bsc.dinhmuc_giao_dthu_ptm --14069433219.713 --14908415173.74
				where thang = 202501 
				;
		update ttkd_bsc.bangluong_kpi a 
					set THUCHIEN = (select round(nvl(KQTH, 0)/1000000, 3) from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = a.thang and ma_nv = a.ma_nv)
--				select * from  ttkd_bsc.bangluong_kpi a
				where ma_kpi in ('HCM_DT_PTMOI_021') and a.thang = 202501 
				;
		----update DTHU thau 2 NV Phong GP, hang thang lien he Phuoc gui
		update ttkd_bsc.bangluong_kpi
			set THUCHIEN = case when ma_nv = 'VNP027259' then THUCHIEN + 29160000/1000000
												when ma_nv = 'VNP017190' then THUCHIEN + 29160000/1000000
												end
--		select * from  ttkd_bsc.bangluong_kpi
		where thang = 202501 and ma_kpi = 'HCM_DT_PTMOI_021' and ma_nv in ('VNP027259', 'VNP017190')
		;
		update ttkd_bsc.bangluong_kpi a set NGAYCONG = 17
				where a.thang = 202501
		;
		---Khong GIAO Hong Duyen - BHOL
		delete from ttkd_bsc.bangluong_kpi a where ma_kpi in ('HCM_DT_PTMOI_021') and a.thang = 202501
							and ma_nv  in ('VNP017072')
		;
		---
		update ttkd_bsc.bangluong_kpi a set CHITIEU_GIAO = 100, donvi_tinh = '%'
--			select * from ttkd_bsc.bangluong_kpi a 
				where ma_kpi in ('HCM_DT_PTMOI_021') and a.thang = 202501 
		;
		update ttkd_bsc.bangluong_kpi a set GIAO = 
																					case 
																							when ma_vtcv in ('VNP-HNHCM_GP_3') then 16		---fix so theo vb ap dung T11
																							when ma_vtcv in ('VNP-HNHCM_GP_3.4') then 16		---fix so theo vb ap dung T11
																							when ma_vtcv in ('VNP-HNHCM_KHDN_18') then 16		---fix T11 là 16tr
																							when ma_vtcv in ('VNP-HNHCM_KDOL_4') then 14.5		---fix so theo vb ap dung T8
																							when ma_vtcv in ('VNP-HNHCM_KDOL_5') then 87.5		---fix so theo vb ap dung T8_ Vinh y/c tren Group Xu ly tinh tren 5 nvien, Tiên hok co
																							when ma_vtcv in ('VNP-HNHCM_KDOL_17.1') then 80		---file giao PBHOL T01
																							when ma_nv = 'VNP017740' and ma_vtcv in ('VNP-HNHCM_KDOL_2') then 206.302	--file giao anh Tuyết  PGP  PBHOL 202501

																							when ma_vtcv in ('VNP-HNHCM_KDOL_17') then 5.6		---fix so theo vb ap dung T9, nhung chua bik vi tri nao 2.6tr																							
																							when ma_vtcv in ('VNP-HNHCM_BHKV_53') then 5.6 		---Fix so theo vb ap dung T8 lay theo dinh muc, 430 để đánh giá P1
																							when ma_vtcv in ('VNP-HNHCM_BHKV_52') then  		---TT BHOL, Fix so theo vb ap dung T8 lay theo 430, tối thiểu >= đinh mực nvien thực tế
																										(select case when nvl(TONG_DTGIAO, 0) < DINHMUC_2 then  round(DINHMUC_2/1000000, 3)
																														else round(TONG_DTGIAO/1000000, 3) end
																											from ttkd_bsc.dinhmuc_giao_dthu_ptm 
																														where thang = a.thang and ma_nv = a.ma_nv 
																										)
																								else	(select round(TONG_DTGIAO/1000000, 3) from ttkd_bsc.dinhmuc_giao_dthu_ptm 
																											where thang = a.thang and ma_nv = a.ma_nv 
																														--	and trunc(dateinput) = '28/07/2024'
																										)
																					end
--				select * from ttkd_bsc.bangluong_kpi a
				where a.ma_kpi in ('HCM_DT_PTMOI_021') and a.thang = 202501-- and ma_pb != 'VNP0703000'
				and ma_vtcv in ('VNP-HNHCM_BHKV_1', 'VNP-HNHCM_BHKV_2', 'VNP-HNHCM_BHKV_2.1')
--				and ma_to = 'VNP0701104'
			;
		update ttkd_bsc.bangluong_kpi a set tytrong = case when ma_vtcv in ('VNP-HNHCM_GP_3') then 40	---fix so theo vb ap dung T8 NV PS PGP
																								when ma_vtcv in ('VNP-HNHCM_GP_3.4') then 80		---fix so theo vb ap dung T11
																								when ma_vtcv in ('VNP-HNHCM_KHDN_18') then 90	---fix so theo vb ap dung T1 NV AM chuyen ban BHDN
																								
																								when ma_vtcv in ('VNP-HNHCM_KDOL_4', 'VNP-HNHCM_KDOL_5') then 95		---fix so theo vb ap dung T01/25 NV KDOL, TT KDOL
																								when ma_vtcv in ('VNP-HNHCM_KDOL_17') then 50		---fix so theo vb ap dung T8	NV OB BH
																								when ma_vtcv in ('VNP-HNHCM_KDOL_3.1', 'VNP-HNHCM_KDOL_17.1') then 40		---fix so theo vb ap dung T1/25	Truong ca OB BH
																								when ma_vtcv in ('VNP-HNHCM_BHKV_6') then 90	---fix so theo vb ap dung T1/25 KDDB
																								when ma_vtcv in ('VNP-HNHCM_BHKV_42') then 100	---fix so theo vb ap dung T1/25  TT KDDB
																								when ma_vtcv in ('VNP-HNHCM_BHKV_1', 'VNP-HNHCM_BHKV_2.1') then 70	---fix so theo vb ap dung GD KV và PT KV - CH T1/25
																								when ma_vtcv in ('VNP-HNHCM_BHKV_2') then 100	---fix so theo vb ap dung PGD KV_banhang BHOL_CH, BRCD_CNTT T1/25
																								when ma_vtcv in ('VNP-HNHCM_BHKV_41') then 90	--thay doi theo thang AM BHKV
																								when ma_vtcv in ('VNP-HNHCM_BHKV_51') then 100	--thay doi theo thang TT CNTT KV
																								when ma_vtcv in ('VNP-HNHCM_BHKV_53') then 90	--thay doi theo thang NV OBBH - BHKV
																								when ma_vtcv in ('VNP-HNHCM_BHKV_52') then 100	--thay doi theo thang TT OBBH - BHKV
																								when ma_vtcv in ('VNP-HNHCM_BHKV_22') then 70	--thay doi theo thang GDV T1/25
																								when ma_vtcv in ('VNP-HNHCM_BHKV_28', 'VNP-HNHCM_BHKV_27') then 80	--thay doi theo thang CHT, CHT kGDV T1/25
																								----vi tri PGD theo vb va theo bang phan cong KPI nao, sheet nao trong vb
																								when ma_nv in ('VNP017740') and ma_vtcv in ('VNP-HNHCM_KDOL_2') then 45	--PGD OL Tuyet
--																								when ma_nv in ('VNP017721', 'VNP017813', 'VNP020231', 'VNP017528', 'VNP019931', 'VNP017853', 'VNP017601', 'VNP017604', 'VNP017305')
--																												then 80		---sheet PGD PT BH
--																								when ma_nv in ('VNP001757', 'VNP001724', 'VNP019529', 'VNP017948') then 100	---sheet PGD PT BH Ko PT BRCĐ
--																								when ma_nv in ('VNP016983') then 60 ---sheet PGD PT BH-CSKH-CH
--																								when ma_nv in ('VNP017496', 'VNP017585', 'VNP017947', 'VNP017729', 'VNP016659', 'VNP016898') then 70---sheet PGD PT BH-CSKH
																								
																						 end
--				select * from ttkd_bsc.bangluong_kpi a
				where a.ma_kpi in ('HCM_DT_PTMOI_021') and a.thang = 202501-- and ma_pb != 'VNP0703000'
			;
		update ttkd_bsc.bangluong_kpi a set TYLE_THUCHIEN = case when GIAO = 0 then null
																												when ma_vtcv in ('VNP-HNHCM_BHKV_6', 'VNP-HNHCM_BHKV_41') ----KDDB, AM BHKV
																																		and giao < 13 and thuchien < 13 and ROUND(THUCHIEN/GIAO*100, 2) > 100
																															then 100
																												when ma_vtcv in ('VNP-HNHCM_BHKV_6', 'VNP-HNHCM_BHKV_41') ----KDDB, AM BHKV
																																		and giao < 13 and thuchien >= 13 
																															then ROUND(THUCHIEN/13 *100, 2)	
																												when ma_vtcv in ('VNP-HNHCM_BHKV_6', 'VNP-HNHCM_BHKV_41') ----KDDB, AM BHKV
																																		and giao > 16				---so voi muc chuan 1
																															then ROUND(THUCHIEN/16 *100, 2)
																															
																												when ma_vtcv in ('VNP-HNHCM_BHKV_22') ----GDV
																																		and giao < 8 and thuchien < 8 and ROUND(THUCHIEN/GIAO*100, 2) > 100
																															then 100
																												when ma_vtcv in ('VNP-HNHCM_BHKV_22') ----GDV
																																		and giao < 8 and thuchien >= 8 
																															then ROUND(THUCHIEN/8 *100, 2)
																												when ma_vtcv in ('VNP-HNHCM_BHKV_22') ----GDV
																																		and giao >10  				---so voi muc chuan 1
																															then ROUND(THUCHIEN/10 *100, 2)
																															
																												when ma_vtcv in ('VNP-HNHCM_BHKV_53') ----OB BHKV
																																		and giao < 5.6 and thuchien < 5.6 and ROUND(THUCHIEN/GIAO*100, 2) > 100
																															then 100
																												when ma_vtcv in ('VNP-HNHCM_BHKV_53') ----OB BHKV
																																		and giao < 5.6 and thuchien >= 5.6 
																															then ROUND(THUCHIEN/5.6 *100, 2)
																												when ma_vtcv in ('VNP-HNHCM_BHKV_53') ----OB BHKV
																																		and giao > 7  				---so voi muc chuan 1
																															then ROUND(THUCHIEN/7 *100, 2)
																												
																												when ma_vtcv in ('VNP-HNHCM_BHKV_27', 'VNP-HNHCM_BHKV_28'
																																				, 'VNP-HNHCM_BHKV_51', 'VNP-HNHCM_BHKV_42') ----CHT/kGDV, TT CNTT, TT KDDB
																																		and giao < (select nvl(DINHMUC_2, 0) / 1000000
																																							from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = a.thang and ma_nv = a.ma_nv) 
																																		and thuchien < (select nvl(DINHMUC_2, 0) / 1000000
																																							from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = a.thang and ma_nv = a.ma_nv) 
																																		and ROUND(THUCHIEN/GIAO*100, 2) > 100
																															then 100
																												when ma_vtcv in ('VNP-HNHCM_BHKV_27', 'VNP-HNHCM_BHKV_28'
																																				, 'VNP-HNHCM_BHKV_51', 'VNP-HNHCM_BHKV_42') ----CHT/kGDV, TT CNTT, TT KDDB
																																		and giao < (select nvl(DINHMUC_2, 0) /1000000
																																										from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = a.thang and ma_nv = a.ma_nv)  
																																		and thuchien >= (select nvl(DINHMUC_2, 0) /1000000
																																										from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = a.thang and ma_nv = a.ma_nv)  
																															then ROUND(THUCHIEN/(select nvl(DINHMUC_2, 0) /1000000
																																										from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = a.thang and ma_nv = a.ma_nv)
																																							*100, 2)
																												
																												else ROUND(THUCHIEN/GIAO*100, 2) 
																									end
--				select * from ttkd_bsc.bangluong_kpi a
				where ma_kpi in ('HCM_DT_PTMOI_021') and thang = 202501  and ma_vtcv in ('VNP-HNHCM_BHKV_1', 'VNP-HNHCM_BHKV_2', 'VNP-HNHCM_BHKV_2.1') 
			;
		update ttkd_bsc.bangluong_kpi a set MUCDO_HOANTHANH = case 
																														--- case: khong danh gia BSC
																														when exists (select * from ttkd_bsc.nhanvien where thang = a.thang and ma_nv = a.ma_nv and tinh_bsc = 0)
																																	then 100
																														----BHOL (NV OB_BH)
																														when ma_vtcv in ('VNP-HNHCM_KDOL_17') and TYLE_THUCHIEN <= 70
																																	then round(0.85 * TYLE_THUCHIEN, 2)			-- 85% *TLTH
																														----BHKV (ALL), PGP (NV PS), BHOL (NV KDOL, TT KDOL)
																														when TYLE_THUCHIEN < 30 then 0		-- 0%
																														when TYLE_THUCHIEN >= 30 and TYLE_THUCHIEN <= 70
																																	then round(0.85 * TYLE_THUCHIEN, 2)			-- 85% *TLTH --> chi Vuong Viber y/c delete * ty trong
																														when TYLE_THUCHIEN > 70 and TYLE_THUCHIEN <= 100
																																	then round(1 * TYLE_THUCHIEN, 2)				-- 100% *TLTH --> chi Vuong Viber y/c delete * ty trong
																														when ma_vtcv in ('VNP-HNHCM_GP_3', 'VNP-HNHCM_GP_3.4') 
																																	and TYLE_THUCHIEN > 100
																																	then case when 100 + (1.2 * (TYLE_THUCHIEN - 100)) > 150 then 150
																																						else round(100 + (1.2 * (TYLE_THUCHIEN - 100)), 2) end ---100% + 1.2 x (TLTH � 100%) -- max 150% dv PGP
																														when TYLE_THUCHIEN > 100
																																	then case when 100 + (1.2 * (TYLE_THUCHIEN - 100)) > 200 then 200
																																						else round(100 + (1.2 * (TYLE_THUCHIEN - 100)), 2) end ---100% + 1.2 x (TLTH � 100%) -- max 150%
																												end
--				select * from ttkd_bsc.bangluong_kpi a
				where ma_kpi in ('HCM_DT_PTMOI_021') and a.thang = 202501 and ma_vtcv in ('VNP-HNHCM_BHKV_1', 'VNP-HNHCM_BHKV_2', 'VNP-HNHCM_BHKV_2.1') 
--							and ma_nv = 'VNP019532'
--and ma_nv in ('VNP016950', 'VNP001757', 'VNP017203', 'VNP016659', 'HCM004899', 'VNP019529')
			;
		---Update ket qua chi AUCO --> a Ba Vu (dac thu)
		update ttkd_bsc.bangluong_kpi a 
						set (giao, thuchien, tyle_thuchien, mucdo_hoanthanh) = (select giao, thuchien, tyle_thuchien, mucdo_hoanthanh 
																														from ttkd_bsc.bangluong_kpi where ma_nv = 'VNP016730' and ma_kpi = a.ma_kpi and a.thang = thang)
								, tytrong = 100
		where ma_nv = 'VNP017014' and ma_kpi in ('HCM_DT_PTMOI_021') and thang = 202501
		;
		select * from ttkd_bsc.bangluong_kpi where ma_kpi in ('HCM_DT_PTMOI_021') and thang = 202501
		and ma_nv in ('VNP017014', 'VNP016730')
--		where ma_nv = 'CTV028802'
		;
commit;
rollback;
		
;
		----tinh heso doanhthu de tinh dongia
		select * from ttkd_bsc.bldg_danhmuc_vtcv_p1 where thang = 202501;
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
		WHERE a.thang = 202501 and loai_kpi = 'KPI_CHT_GDV' 
		;
				--CH chi co 1 CHT_kGDV
		UPDATE  ttkd_bsc.dinhmuc_giao_dthu_ptm a set a.canhan_thuchien = a.kqth
		WHERE a.thang = 202501 and loai_kpi = 'KPI_CHT_GDV' and canhan_thuchien is null
		;
		MERGE INTO ttkd_bsc.dinhmuc_giao_dthu_ptm a
				USING ttkd_bsc.bldg_danhmuc_vtcv_p1 b
				ON (a.thang = b.thang and a.ma_vtcv = b.ma_vtcv)
		WHEN MATCHED THEN
		update  
							set HESO_QD_DT_PTM = case when KHDK < b.dinhmuc_3 and canhan_thuchien < b.dinhmuc_3 then 0.8 --12
																				 when KHDK < b.dinhmuc_3 and canhan_thuchien < b.dinhmuc_2 and canhan_thuchien >= b.dinhmuc_3 then 0.85	--11
																				when KHDK < b.dinhmuc_2 and KHDK >= b.dinhmuc_3 and canhan_thuchien < b.dinhmuc_3 then 0.85	--9
																				when KHDK < b.dinhmuc_3 and canhan_thuchien >= b.dinhmuc_2 then 0.9		--10
																				when KHDK < b.dinhmuc_2 and KHDK >= b.dinhmuc_3 and canhan_thuchien < b.dinhmuc_2 and canhan_thuchien >= b.dinhmuc_3 then 0.9		--8
																				when KHDK < b.dinhmuc_1  and KHDK >= b.dinhmuc_2 and canhan_thuchien < b.dinhmuc_3 then 0.9		--6
																				when KHDK < b.dinhmuc_2 and KHDK >= b.dinhmuc_3 and canhan_thuchien >= b.dinhmuc_2 then 0.95		--7
																				when KHDK < b.dinhmuc_1 and KHDK >= b.dinhmuc_2 and canhan_thuchien < b.dinhmuc_2 and canhan_thuchien >= b.dinhmuc_3 then 0.95	--5
																				when KHDK >= b.dinhmuc_1 and canhan_thuchien < b.dinhmuc_2 then 0.95		--3
																				when KHDK < b.dinhmuc_1 and KHDK >= b.dinhmuc_2 and canhan_thuchien >= b.dinhmuc_2 then 1  --4
																				when KHDK >= b.dinhmuc_1 and canhan_thuchien < b.dinhmuc_1 and canhan_thuchien >= b.dinhmuc_2 then 1		--2
																				when KHDK >= b.dinhmuc_1 and canhan_thuchien >= b.dinhmuc_1 then 1.1		--1
																	else null
																				 end
--					select * from ttkd_bsc.dinhmuc_giao_dthu_ptm a
					where a.ma_vtcv in ('VNP-HNHCM_BHKV_27') and a.thang = 202501 
			;
			update ttkd_bsc.dinhmuc_giao_dthu_ptm 
							set HESO_QD_DT_PTM = case when KHDK < dinhmuc_3 and KQTH < dinhmuc_3 then 0.8 --12
																				 when KHDK < dinhmuc_3 and KQTH < dinhmuc_2 and KQTH >= dinhmuc_3 then 0.85	--11
																				when KHDK < dinhmuc_2 and KHDK >= dinhmuc_3 and KQTH < dinhmuc_3 then 0.85	--9
																				when KHDK < dinhmuc_3 and KQTH >= dinhmuc_2 then 0.9		--10
																				when KHDK < dinhmuc_2 and KHDK >= dinhmuc_3 and KQTH < dinhmuc_2 and KQTH >= dinhmuc_3 then 0.9		--8
																				when KHDK < dinhmuc_1  and KHDK >= dinhmuc_2 and KQTH < dinhmuc_3 then 0.9		--6
																				when KHDK < dinhmuc_2 and KHDK >= dinhmuc_3 and KQTH >= dinhmuc_2 then 0.95		--7
																				when KHDK < dinhmuc_1 and KHDK >= dinhmuc_2 and KQTH < dinhmuc_2 and KQTH >= dinhmuc_3 then 0.95	--5
																				when KHDK >= dinhmuc_1 and KQTH < dinhmuc_2 then 0.95		--3
																				when KHDK < dinhmuc_1 and KHDK >= dinhmuc_2 and KQTH >= dinhmuc_2 then 1  --4
																				when KHDK >= dinhmuc_1 and KQTH < dinhmuc_1 and KQTH >= dinhmuc_2 then 1		--2
																				when KHDK >= dinhmuc_1 and KQTH >= dinhmuc_1 then 1.1		--1
																	else null
																				 end
--					select distinct ten_vtcv from ttkd_bsc.nhanvien
					where thang = 202501
								and ma_vtcv  in ('VNP-HNHCM_BHKV_15', 'VNP-HNHCM_BHKV_15.1', 'VNP-HNHCM_BHKV_22', 'VNP-HNHCM_BHKV_41', 'VNP-HNHCM_BHKV_53', 'VNP-HNHCM_BHKV_6') 
								and ma_pb not in (
																'VNP0702300',
																'VNP0702400',
																'VNP0702500'
																) 
;
		---Ap dung vb 292 dv BHDN eO 552546
		update ttkd_bsc.dinhmuc_giao_dthu_ptm a 
						set HESO_QD_DT_PTM = (select 
																		case when TYLE_THUCHIEN < 80 then 0.8
																				when TYLE_THUCHIEN >= 80 and TYLE_THUCHIEN < 90 then 0.85
																				when TYLE_THUCHIEN >= 90 and TYLE_THUCHIEN < 95 then 0.9
																				when TYLE_THUCHIEN >= 95 and TYLE_THUCHIEN < 98 then 0.95
																				when TYLE_THUCHIEN >= 98 and TYLE_THUCHIEN < 100 then 1
																				when TYLE_THUCHIEN >= 100 then 1.1 else null end heso_qd_dthu
																from ttkd_bsc.bangluong_kpi 
																where ma_kpi in ('HCM_DT_LUYKE_002', 'HCM_DT_PTMOI_021') and thang = a.thang and ma_nv = a.ma_nv
															)
--			select * from ttkd_bsc.dinhmuc_giao_dthu_ptm a 															
			where thang = 202501
						and ma_vtcv  in ('VNP-HNHCM_KHDN_23', 'VNP-HNHCM_KHDN_3.1', 'VNP-HNHCM_KHDN_18', 'VNP-HNHCM_KHDN_3') 
						and ma_pb in (
											'VNP0702300',
											'VNP0702400',
											'VNP0702500'
											) 
										--	and ma_nv in (select ma_nv from ttkd_bsc.bang_heso_dthu where thang >= 202409 and ma_vtcv = 'VNP-HNHCM_KHDN_18' and HESO_DTHU is null)
			;
	
	rollback;
	commit;
------------------------------------------------------------------------------------------------

select * from ttkd_bsc.bldg_danhmuc_vtcv_p1 where thang = 202501;
select *
		from admin.v_hs_thuebao a
					join admin.v_file_hs b on a.FILE_ID = b.FILE_ID
					;

		
		


