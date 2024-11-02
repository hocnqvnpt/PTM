/*
create table bangluong_kpi_202409_l6 as select * from bangluong_kpi_202409;  


ktra sau khi add du lieu thang do Xuan Vinh cung cap
select * from ttkd_bsc.dinhmuc_dthu_ptm where  thang=202409 ;
update ttkd_bsc.dinhmuc_dthu_ptm set  thang=202409 where thang is null;
update ttkd_bsc.dinhmuc_dthu_ptm set dt_giao_bsc='' where thang=202409 and dt_giao_bsc=0;
*/
create table ttkd_bsc.bangluong_kpi_202409_dot2 as select * from ttkd_bsc.bangluong_kpi_202409;  
create table ttkd_bsc.bangluong_kpi_20241022_dot3 as select * from ttkd_bsc.bangluong_kpi where thang = 202409;  
select * from ttkd_bsc.bangluong_kpi_dot2_20240920 where thang = 202409;  

select distinct a.*, b.*, c.ten_vtcv 
		from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien c
        where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv and c.thang= 202409
                    and a.thang = b.thang and a.thang = c.thang
                    and a.ma_kpi='HCM_DT_PTMOI_021' ;

--      select * from ttkd_bsc.temp_trasau_canhan     
          
		drop table ttkd_bsc.temp_trasau_canhan purge;
		desc ttkd_bsc.ghtt_vnpts;
		;4817.4409992
		select sum(dthu_kpi)/1000000
		from ttkd_bsc.temp_trasau_canhan a
							join ttkd_bsc.nhanvien nv on nv.thang = 202409 and a.ma_nv = nv.ma_nv
		where nv.ma_pb in ('VNP0701100', 'VNP0701200', 'VNP0701300', 'VNP0701400', 'VNP0701500','VNP0701600', 'VNP0701800', 'VNP0702100', 'VNP0702200')
							 ;VNP017346
		----Tat ca dich vu, ngoai tru VNPtt
		create table ttkd_bsc.temp_trasau_canhan as
--		insert into ttkd_bsc.temp_trasau_canhan --(MANV_PTM, DTHU_KPI)
				select DICH_VU, LOAITB_ID, dichvuvt_id, cast(manv_ptm as varchar(20)) ma_nv,  sum(dthu_kpi) dthu_kpi
				from (
					
						---DNHM cho cot NVPTM
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_ptm, doanhthu_kpi_dnhm * heso_hotro_nvptm dthu_kpi
						from ttkd_bsc.ct_bsc_ptm
						where thang_tlkpi_dnhm = 202409 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_dnhm >0
					union all
					---DNHM cho cot NVHOTRO kenh ngoai
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_hotro, doanhthu_kpi_dnhm * heso_hotro_nvhotro doanhthu_kpi_dnhm
						from ttkd_bsc.ct_bsc_ptm
						where thang_tlkpi_hotro = 202409 
									and tyle_am is null and tyle_hotro is null and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_dnhm >0
					union all
					---DNHM cho cot DAI
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_tt_dai, doanhthu_kpi_dnhm * heso_hotro_dai doanhthu_kpi_dnhm
						from ttkd_bsc.ct_bsc_ptm
						where thang_tldg_dt_dai = 202409 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_dnhm >0
					union all
						---dich vu ngoai ctr OR dvu khac VNPtt
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_ptm, doanhthu_kpi_nvptm dthu_kpi
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi = 202409 and (loaitb_id<>21 or ma_kh='GTGT rieng')
										and doanhthu_kpi_nvptm >0
					union all
					----Dthu nvho tro ban kenh ngoai
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_hotro, doanhthu_kpi_nvhotro 
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi_hotro = 202409
										and tyle_am is null and tyle_hotro is null and (loaitb_id<>21 or ma_kh='GTGT rieng')
										and doanhthu_kpi_nvhotro >0
					union all
					---Dthu nvhotro PGP
						select cast(thang_ptm as number) thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_hotro, doanhthu_kpi_nvhotro
						from ttkd_bsc.ct_bsc_ptm_pgp 
						where thang_tlkpi_hotro=202409 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_nvhotro >0
					union all
					---Dthu nv DAI
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_tt_dai, doanhthu_kpi_nvdai 
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tldg_dt_dai = 202409 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_nvdai >0
					union all
					----Gia han VNPts
						select thang, ma_kh, ma_tb, 'VNPTS' dich_vu, 20 loaitb_id, 2 dichvuvt_id, ma_nv, doanhthu_dongia 
						from ttkd_bsc.ghtt_vnpts 		--- Nguyen quan ly, chua co vb, toan noi mieng
						where thang=202409 and thang_giao is not null and ma_nv is not null
					union all
					----GHTT toBHOL BHKV (Nhu Y)
						select a.THANG, null ma_gd, 'ghtt' ma_tb, 'Fiber', 58, 4, a.MA_NV, sum(TIEN) dthu_kpi
						from ttkd_Bsc.tl_Giahan_Tratruoc a
									join ttkd_bsc.nhanvien nv on a.thang = nv.thang and a.ma_nv = nv.ma_nv
						where a.thang = 202409 and loai_tinh = 'DOANHTHU'
									and nv.ma_vtcv in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53')
						group by a.THANG, a.MA_NV
				)
--				where MANV_PTM in ('CTV071620','VNP027259')
				group by DICH_VU, LOAITB_ID, dichvuvt_id, manv_ptm
		;
	create index ttkd_bsc.temp_trasau_canhan_manv on ttkd_bsc.temp_trasau_canhan (ma_nv)
	;
-- to truong: thieu 0021_ts
	drop table ttkd_bsc.temp_totruong purge;
	
	select sum(dthu_kpi)/1000000 from ttkd_bsc.temp_totruong 
	where ma_pb in ('VNP0701100', 'VNP0701200', 'VNP0701300', 'VNP0701400', 'VNP0701500','VNP0701600', 'VNP0701800', 'VNP0702100', 'VNP0702200');
	select sum(dthu_kpi) dthu_to, 0 dthu_cn from ttkd_bsc.temp_totruong where ma_to = 'VNP0701406'; 9 670 690 982.4 (10 999 250 012.15 )
	union all 
	;
	select a.ma_nv, nv.ten_nv, ten_to, ten_pb, ten_vtcv, sum(dthu_kpi)
	from ttkd_bsc.temp_trasau_canhan a
					join ttkd_bsc.nhanvien nv on a.ma_nv = nv.ma_nv and nv.thang = 202409
			where nv.ma_to = 'VNP0701406'
	group by a.ma_nv, nv.ten_nv, ten_to, ten_pb, ten_vtcv
	;
	create table ttkd_bsc.temp_totruong as
			select ma_pb, ma_to, loaitb_id, dichvuvt_id, sum(doanhthu_kpi_to) dthu_kpi
			from(
					---- To NVPTM
					select ma_pb, ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_to * heso_hotro_nvptm as doanhthu_kpi_to
					  from ttkd_bsc.ct_bsc_ptm a
					  where thang_tlkpi_to = 202409 and (loaitb_id<>21 or loaitb_id is null) 
									and doanhthu_kpi_to >0
				union all
					---- To NVHOTRO DIGISHOP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_to * heso_hotro_nvhotro as doanhthu_kpi_to
					  from ttkd_bsc.ct_bsc_ptm a
									join ttkd_bsc.nhanvien b on a.thang_tlkpi_to = b.thang and a.manv_hotro = b.ma_nv
					  where thang_tlkpi_to = 202409 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null 
									and doanhthu_kpi_to >0
									and nvl(vanban_id, 0) != 764  ---only T7, 8,9, thang sau xoa
				union all
					---To  Nvien Cot MANV_HOTRO PGP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_nvhotro
					  from ttkd_bsc.ct_bsc_ptm a
									join ttkd_bsc.nhanvien b on a.thang_tlkpi_hotro = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_hotro = 202409 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is not null and tyle_hotro is not null 
									and doanhthu_kpi_nvhotro >0
--				 ---MANV_DAI
					union all
						select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_to * heso_hotro_dai
						from ttkd_bsc.ct_bsc_ptm a
										join ttkd_bsc.nhanvien b on a.thang_tlkpi_to = b.thang and a.manv_tt_dai = b.ma_nv
						where thang_tlkpi_to = 202409 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_to >0
									and nvl(vanban_id, 0) != 764  ---only T7, 8,9, thang sau xoa
				union all
					---To  Nvien Cot MANV_PTM cho dthu DNHM
					select ma_pb, ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm_phong * heso_hotro_nvptm as doanhthu_kpi_dnhm
					  from ttkd_bsc.ct_bsc_ptm a
					 where thang_tlkpi_dnhm_to = 202409 and (loaitb_id<>21 or loaitb_id is null)
									and doanhthu_kpi_dnhm_phong >0
				union all
					---To  Nvien Cot MANV_HOTRO cho dthu DNHM tren DIGISHOP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm_phong * heso_hotro_nvhotro as doanhthu_kpi_dnhm
					  from ttkd_bsc.ct_bsc_ptm a
								join ttkd_bsc.nhanvien b on a.thang_tlkpi_dnhm_to = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_dnhm_to = 202409 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null
									and doanhthu_kpi_dnhm_phong >0
									and nvl(vanban_id, 0) != 764 ---only T7,8,9, thang sau xoa
--				union all ---dthu DNHM PGP không có tinh Tổ trưởng
				union all
					select ma_pb, ma_to, ma_tb, 20 loaitb_id, 2 dichvuvt_id, doanhthu_dongia 
					from ttkd_bsc.ghtt_vnpts		----a Nguyen quan ly
					  where thang=202409 and thang_giao is not null and ma_to is not null 
				
				union all
					----GHTT toBHOL BHKV (Nhu Y)
						select a.ma_pb, a.ma_to, 'ghtt' ma_tb, 58, 4, sum(TIEN) dthu_kpi
						from ttkd_Bsc.tl_Giahan_Tratruoc a
									join ttkd_bsc.nhanvien nv on a.thang = nv.thang and a.ma_nv = nv.ma_nv
						where a.thang = 202409 and loai_tinh = 'DOANHTHU'
									and nv.ma_vtcv in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53')
						group by a.ma_pb, a.ma_to
					  
						) group by ma_pb, ma_to, loaitb_id, dichvuvt_id
		;

						 
		
---- ldp phu trach: tinh theo MA_TO va NHOM dich vu
		drop table ttkd_bsc.x_temp_phong purge;
		create table ttkd_bsc.x_temp_phong as
			select ma_pb, ma_to, loaitb_id, dichvuvt_id, sum(doanhthu_kpi_phong) dthu_kpi
			from(
					---- To NVPTM
					select ma_pb, ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_phong * heso_hotro_nvptm as doanhthu_kpi_phong
					  from ttkd_bsc.ct_bsc_ptm a
					  where thang_tlkpi_phong = 202409 and (loaitb_id<>21 or loaitb_id is null) 
									and doanhthu_kpi_phong >0
				union all
					---- To NVHOTRO DIGISHOP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_phong * heso_hotro_nvhotro as doanhthu_kpi_phong
					  from ttkd_bsc.ct_bsc_ptm a
									join ttkd_bsc.nhanvien b on a.thang_tlkpi_phong = b.thang and a.manv_hotro = b.ma_nv
					  where thang_tlkpi_phong = 202409 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null 
									and doanhthu_kpi_phong >0
									and nvl(vanban_id, 0) != 764 ---only T7,8,9, thang sau xoa
				union all
					---To  Nvien Cot MANV_HOTRO PGP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_phong * heso_hotro_nvhotro as doanhthu_kpi_phong
					  from ttkd_bsc.ct_bsc_ptm a
									join ttkd_bsc.nhanvien b on a.thang_tlkpi_phong = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_phong = 202409 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is not null and tyle_hotro is not null 
									and doanhthu_kpi_phong >0
--				 ---MANV_DAI
					union all
						select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_phong * heso_hotro_dai as doanhthu_kpi_phong
						from ttkd_bsc.ct_bsc_ptm a
										join ttkd_bsc.nhanvien b on a.thang_tlkpi_phong = b.thang and a.manv_tt_dai = b.ma_nv
						where thang_tlkpi_phong = 202409 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_phong >0
									and nvl(vanban_id, 0) != 764  ---only T7, 8,9, thang sau xoa
				union all
					---************DNHM*****To  Nvien Cot MANV_PTM cho dthu DNHM
					select ma_pb, ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm_phong * heso_hotro_nvptm as doanhthu_kpi_dnhm_phong
					  from ttkd_bsc.ct_bsc_ptm a
					 where thang_tlkpi_dnhm_phong = 202409 and (loaitb_id<>21 or loaitb_id is null)
									and doanhthu_kpi_dnhm_phong >0
				union all
					---To  Nvien Cot MANV_HOTRO cho dthu DNHM tren DIGISHOP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm_phong * heso_hotro_nvhotro as doanhthu_kpi_dnhm_phong
					  from ttkd_bsc.ct_bsc_ptm a
								join ttkd_bsc.nhanvien b on a.thang_tlkpi_dnhm_phong = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_dnhm_phong = 202409 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null
									and doanhthu_kpi_dnhm_phong >0
									and nvl(vanban_id, 0) != 764 ---only T7, 8, 9, thang sau xoa
--				union all ---dthu DNHM PGP không có tinh Phó Giam đốc
				union all
					select ma_pb, ma_to, ma_tb, 20 loaitb_id, 2 dichvuvt_id, doanhthu_dongia 
					from ttkd_bsc.ghtt_vnpts		----a Nguyen quan ly
					  where thang=202409 and thang_giao is not null and ma_to is not null 
				
				union all
					----GHTT toBHOL BHKV (Nhu Y)
						select a.ma_pb, a.ma_to, 'ghtt' ma_tb, 58, 4, sum(TIEN) dthu_kpi
						from ttkd_Bsc.tl_Giahan_Tratruoc a
									join ttkd_bsc.nhanvien nv on a.thang = nv.thang and a.ma_nv = nv.ma_nv
						where a.thang = 202409 and loai_tinh = 'DOANHTHU'
									and nv.ma_vtcv in ('VNP-HNHCM_BHKV_52', 'VNP-HNHCM_BHKV_53')
						group by a.ma_pb, a.ma_to
				

						) group by ma_pb, ma_to, loaitb_id, dichvuvt_id
		;
		----PTM
		select sum(DTHU_KPI)  from ttkd_bsc.temp_021_ldp1 
				where --dichvu in ('Mega+Fiber', 'MyTV') and 
--						NHOM_DICHVU  in ('Dichvu') and
						ma_pb in ('VNP0701100', 'VNP0701200', 'VNP0701300', 'VNP0701400', 'VNP0701500','VNP0701600', 'VNP0701800', 'VNP0702100', 'VNP0702200') 
						and ma_to  in (select ma_to from ttkd_bsc.nhanvien where thang = 202409)
						; 4635206943.2
		select * from ttkd_bsc.temp_021_ldp1 where ma_pb in ('VNP0701100', 'VNP0701200', 'VNP0701300', 'VNP0701400', 'VNP0701500','VNP0701600', 'VNP0701800', 'VNP0702100', 'VNP0702200');
		 select sum(DTHU_KPI) from ttkd_bsc.x_temp_phong where ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500'); 9379.411
		 select sum(DTHU_KPI)/1000000 from ttkd_bsc.x_temp_phong 
		 where ma_pb in ('VNP0701100', 'VNP0701200', 'VNP0701300', 'VNP0701400', 'VNP0701500','VNP0701600', 'VNP0701800', 'VNP0702100', 'VNP0702200'); 4638825687.2 4640.3456872
		 
		 drop table  ttkd_bsc.temp_021_ldp1 purge
		 ;
		create table ttkd_bsc.temp_021_ldp1 as
				select a.ma_pb, a.ma_to, b.dv_cap1, b.dv_cap2
							, case when dv_cap1 in ('VNP tra sau', 'VNP tra truoc', 'Mega+Fiber', 'MyTV') then dv_cap1
										when dv_cap2 in ('Dich vu so doanh nghiep') then dv_cap2
									else 'Con lai' end dichvu
							, case when dv_cap1 in ('VNP tra sau', 'VNP tra truoc', 'Mega+Fiber', 'MyTV') then 'Dichvu_cap1'
										when dv_cap2 in ('Dich vu so doanh nghiep') then 'Dichvu_cap2'
									else 'Dichvu' end nhom_dichvu
							, sum(dthu_kpi) dthu_kpi
									from ttkd_bsc.x_temp_phong a
											left join ttkd_bsc.dm_loaihinh_hsqd b on a.loaitb_id = b.loaitb_id 
																and (dv_cap2 in ('Dich vu so doanh nghiep','Truyen so lieu', 'Internet truc tiep') or dv_cap1 in ('VNP tra sau', 'VNP tra truoc', 'Mega+Fiber', 'MyTV'))
											group by a.ma_pb, a.ma_to, b.dv_cap1, b.dv_cap2
			;
		select * from	ttkd_bsc.blkpi_dm_to_pgd pgd
							where pgd.thang=202409 
										and ma_pb in ('VNP0702300', 'VNP0702400', 'VNP0702500')
										;
										
			drop table  ttkd_bsc.temp_021_ldp purge;
	create table ttkd_bsc.temp_021_ldp as	
--			select * from (
			with pgd as ( select distinct MA_PB, ten_to, MA_TO, MA_NV, THANG, dichvu, nhom_dichvu
									from ttkd_bsc.blkpi_dm_to_pgd pgd 
									where thang = 202409 and nhom_dichvu is not null
									)

						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									  join pgd
										on a.ma_to = pgd.ma_to and a.dichvu = pgd.dichvu and a.nhom_dichvu = pgd.nhom_dichvu  and pgd.thang=202409 
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')		---BHKV theo To va Dich vu VNP tra sau
									and a.dichvu = 'VNP tra sau'
						union all
						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									 join pgd
										on a.ma_to = pgd.ma_to and a.dichvu = pgd.dichvu and a.nhom_dichvu = pgd.nhom_dichvu  and pgd.thang=202409 
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')		---BHKV theo To va Dich vu 'Mega, Fiber', 'MyTV'
										and a.dichvu in ('Mega+Fiber', 'MyTV')
						union all
						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									  join pgd
										on a.ma_to = pgd.ma_to and a.dichvu = pgd.dichvu and a.nhom_dichvu = pgd.nhom_dichvu  and pgd.thang=202409 
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')		---BHKV theo To va Dich vu Dich vu so doanh nghiep
										and a.dichvu in ('Dich vu so doanh nghiep')
						union all
						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									  join pgd on a.ma_to = pgd.ma_to and a.dichvu = pgd.dichvu and a.nhom_dichvu = pgd.nhom_dichvu  and pgd.thang=202409 
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')		---BHKV theo To va Dich vu TSL, INT, con lai
									and a.NHOM_DICHVU in ('Dichvu')
--				) --select *  from ttkd_bsc.temp_021_ldp 
--				where ma_nv in ('VNP017072', 'VNP017853')
			;
			---KIEM tra bang sau khi chay
					select sum(dthu_kpi) from
						(
						with pgd as ( select distinct MA_PB, MA_TO, MA_NV, THANG, dichvu, nhom_dichvu
												from ttkd_bsc.blkpi_dm_to_pgd pgd 
												where thang = 202409 and nhom_dichvu is not null
															)
									select a.*, pgd.ma_nv
													, case when exists (select 1 from pgd where ma_pb = a.ma_pb) then 1 else 0 end exit_ma_pb
													, case when exists (select 1 from pgd where ma_to = a.ma_to) then 1 else 0 end exit_ma_to
									from ttkd_bsc.temp_021_ldp1 a
												left join pgd on a.ma_to = pgd.ma_to and a.dichvu = pgd.dichvu and a.nhom_dichvu = pgd.nhom_dichvu and pgd.thang=202409
						) where exit_ma_pb + exit_ma_to = 2 and ma_nv is not null
		;
		commit;
		rollback;
		
		select * from ttkd_bsc.blkpi_danhmuc;
		select * from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = 202409 ;
		select ma_nv, ten_vtcv, hcm_dt_ptmoi_021_vnptt, hcm_dt_ptmoi_021_vnpts, hcm_dt_ptmoi_021_cdbr_mytv, hcm_dt_ptmoi_021_cntt, HCM_DT_PTMOI_021 from ttkd_bsc.bangluong_kpi_202409 a;
		select * from ttkd_bsc.bangluong_kpi a where ma_kpi in ('HCM_DT_PTMOI_021') and thang = 202409;
		select * from ttkdhcm_ktnv.ID430_TTDANGKY_CHOTTHANG where thang = 202409 and manv_hrm = 'VNP016902';
--		update ttkd_bsc.dinhmuc_giao_dthu_ptm a
--					set KHDK = (select TONG_DTGIAO from ttkdhcm_ktnv.ID430_TTDANGKY_CHOTTHANG where thang = 202409 and MANV_HRM = a.ma_nv)
--		where thang = 202409-- and ma_nv = 'CTV021946'
--						and exists (select TONG_DTGIAO from ttkdhcm_ktnv.ID430_TTDANGKY_CHOTTHANG where thang = 202409 and MANV_HRM = a.ma_nv)
				;
--		select * from ttkdhcm_ktnv.ID430_TTDANGKY_CHOTTHANG where thang = 202409 and MANV_HRM not in (select ma_nv from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = 202409)
		;
--		update ttkd_bsc.dinhmuc_giao_dthu_ptm 
--					set NHOMVINATT_KQTH = 1000000 * NHOMVINATT_KQTH
--				where thang = 202409
--				;
		update ttkd_bsc.dinhmuc_giao_dthu_ptm 
					set KQTH = nvl(NHOMBRCD_KQTH, 0) + nvl(NHOMVINATS_KQTH, 0) + nvl(NHOMVINATT_KQTH, 0) + nvl(NHOMCNTT_KQTH, 0) + nvl(NHOMCONLAI_KQTH, 0)
--		select KQTH from ttkd_bsc.dinhmuc_giao_dthu_ptm --14069433219.713 --14908415173.74
				where thang = 202409 
				;
		update ttkd_bsc.bangluong_kpi a 
					set THUCHIEN = (select round(nvl(KQTH, 0)/1000000, 3) from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = a.thang and ma_nv = a.ma_nv)
--				select * from  ttkd_bsc.bangluong_kpi a
				where ma_kpi in ('HCM_DT_PTMOI_021') and a.thang = 202409 
				;
		----update DTHU thau 2 NV Phong GP, hang thang lien he Phuoc gui
		update ttkd_bsc.bangluong_kpi
			set THUCHIEN = case when ma_nv = 'VNP027259' then THUCHIEN + 34480000/1000000
												when ma_nv = 'VNP017190' then THUCHIEN + 34480000/1000000
												end
--		select * from  ttkd_bsc.bangluong_kpi
		where thang = 202409 and ma_kpi = 'HCM_DT_PTMOI_021' and ma_nv in ('VNP027259', 'VNP017190')
		;
		update ttkd_bsc.bangluong_kpi a set NGAYCONG = 23
				where a.thang = 202409
		;
		update ttkd_bsc.bangluong_kpi a set CHITIEU_GIAO = 100
				where ma_kpi in ('HCM_DT_PTMOI_021') and a.thang = 202409
		;
		update ttkd_bsc.bangluong_kpi a set GIAO = 
																					case 
																							when ma_vtcv in ('VNP-HNHCM_GP_3') then 16		---fix so theo vb ap dung T8
																							when ma_vtcv in ('VNP-HNHCM_KDOL_4') then 14.5		---fix so theo vb ap dung T8
																							when ma_vtcv in ('VNP-HNHCM_KDOL_5') then 72.5		---fix so theo vb ap dung T8_ Vinh y/c tren Group Xu ly tinh tren 5 nvien, Tiên hok co
																							
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
--																					(select round(TONG_DTGIAO/1000000, 3) from ttkd_bsc.dinhmuc_giao_dthu_ptm 
--																											where thang = a.thang and ma_nv = a.ma_nv )
--																		, tytrong = case when ma_vtcv in ('VNP-HNHCM_GP_3') then 40	---fix so theo vb ap dung T8 NV PS PGP
--																									when ma_vtcv in ('VNP-HNHCM_KDOL_4', 'VNP-HNHCM_KDOL_5') then 85		---fix so theo vb ap dung T8 NV KDOL, TT KDOL
--																									when ma_vtcv in ('VNP-HNHCM_KDOL_17') then 50		---fix so theo vb ap dung T8	NV OB BH
--																									when ma_nv in ('VNP016799', 'VNP017843') and ma_vtcv = 'VNP-HNHCM_BHKV_6' then 90	--thay doi theo thang TSN KDDB
--																									when ma_vtcv in ('VNP-HNHCM_BHKV_6', 'VNP-HNHCM_BHKV_42') then 80	---fix so theo vb ap dung T8 KDDB, TT KDDB
--																									when ma_vtcv in ('VNP-HNHCM_BHKV_2') then 40	---fix so theo vb ap dung T8 PGD KV _toBHOL
--																									when ma_vtcv in ('VNP-HNHCM_BHKV_2') then 80	---fix so theo vb ap dung T8 PGD KV_banhang
--																									when ma_vtcv in ('VNP-HNHCM_BHKV_41') then 90	--thay doi theo thang AM BHKV
--																									when ma_vtcv in ('VNP-HNHCM_BHKV_51') then 100	--thay doi theo thang TT CNTT KV
--																									when ma_vtcv in ('VNP-HNHCM_BHKV_22') then 50	--thay doi theo thang GDV
--																									when ma_vtcv in ('VNP-HNHCM_BHKV_28', 'VNP-HNHCM_BHKV_27') then 60	--thay doi theo thang CHT, CHT kGDV
--																						 end
--				select * from ttkd_bsc.bangluong_kpi a
				where a.ma_kpi in ('HCM_DT_PTMOI_021') and a.thang = 202409 
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
																												when ma_vtcv in ('VNP-HNHCM_BHKV_27', 'VNP-HNHCM_BHKV_28'
																																				, 'VNP-HNHCM_BHKV_51', 'VNP-HNHCM_BHKV_42') ----CHT/kGDV, TT CNTT, TT KDDB
																																		and giao > (select nvl(DINHMUC_1, 0) /1000000			---so voi muc chuan 1
																																										from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = a.thang and ma_nv = a.ma_nv)  
																															then ROUND(THUCHIEN/(select nvl(DINHMUC_1, 0) /1000000
																																										from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = a.thang and ma_nv = a.ma_nv)
																																							*100, 2)
																												else ROUND(THUCHIEN/GIAO*100, 2) 
																									end
				where ma_kpi in ('HCM_DT_PTMOI_021') and thang = 202409 
			;
		update ttkd_bsc.bangluong_kpi a set CHITIEU_GIAO = 100
																		, MUCDO_HOANTHANH = case 
																														--- case: khong danh gia BSC
																														when exists (select 1 from ttkd_bsc.nhanvien where thang = a.thang and ma_nv = a.ma_nv and tinh_bsc = 0)
																																	then 100
																														----BHKV (ALL), PGP (NV PS), BHOL (NV OB_BH, NV KDOL, TT KDOL)
																														when TYLE_THUCHIEN < 30 then 0		-- 0%
																														when TYLE_THUCHIEN >= 30 and TYLE_THUCHIEN <= 70
																																	then round(0.85 * TYLE_THUCHIEN, 2)			-- 85% *TLTH --> chi Vuong Viber y/c delete * ty trong
																														when TYLE_THUCHIEN > 70 and TYLE_THUCHIEN <= 100
																																	then round(1 * TYLE_THUCHIEN, 2)				-- 100% *TLTH --> chi Vuong Viber y/c delete * ty trong
																														when TYLE_THUCHIEN > 100
																																	then case when 100 + (1.2 * (TYLE_THUCHIEN - 100)) > 150 then 150
																																						else round(100 + (1.2 * (TYLE_THUCHIEN - 100)), 2) end ---100% + 1.2 x (TLTH � 100%) -- max 150%
																												end
																													
				where ma_kpi in ('HCM_DT_PTMOI_021') and a.thang = 202409  --and ma_nv = 'VNP016902'
				
			;
		select * from ttkd_bsc.bangluong_kpi where ma_kpi in ('HCM_DT_PTMOI_021') and thang = 202409
--		where ma_nv in ('VNP027259', 'VNP017190')
--		where ma_nv = 'CTV028802'
		;
commit;
rollback;
		
		create table ttkd_bsc.bangluong_kpi_202409_1021_2330 as select * from ttkd_bsc.bangluong_kpi where thang = 202409;
		select * from  ttkd_bsc.bangluong_kpi where thang = 202409 and ma_kpi = 'HCM_DT_PTMOI_021' and ma_nv = 'CTV051559';
	
		select sum(HCM_DT_PTMOI_021) from  ttkd_bsc.bangluong_kpi_202409 where HCM_DT_PTMOI_021 is not null and ma_nv = 'CTV028802';
		;
		select a.ma_nv, a.ten_nv, a.ten_vtcv, a.ten_donvi, a.HCM_DT_PTMOI_021 HCM_DT_PTMOI_021_newa, b.HCM_DT_PTMOI_021 old
		from ttkd_bsc.bangluong_kpi_202409 a
				join ttkd_bsc.bangluong_kpi_202409_l3 b on a.ma_nv = b.ma_nv
		where a.HCM_DT_PTMOI_021 != b.HCM_DT_PTMOI_021
;
		----tinh heso doanhthu de tinh dongia
		select * from ttkd_bsc.bldg_danhmuc_vtcv_p1 where thang = 202410;
		---Heso dthu CHT kGDV lay theo muc chuan trong bang ttkd_bsc.bldg_danhmuc_vtcv_p1
		----Update Heso dthu tu 2 table ttkd_bsc.dinhmuc_giao_ptm va TLTH chi tieu 1 KHDN
			---Ap dung vb 323 dv BHKV	
		MERGE INTO ttkd_bsc.dinhmuc_giao_dthu_ptm a
				USING ttkd_bsc.bldg_danhmuc_vtcv_p1 b
				ON (a.thang = b.thang and a.ma_vtcv = b.ma_vtcv)
		WHEN MATCHED THEN
		update  
							set HESO_QD_DT_PTM = case when KHDK < b.dinhmuc_3 and KQTH < b.dinhmuc_3 then 0.8 --12
																				 when KHDK < b.dinhmuc_3 and KQTH < b.dinhmuc_2 and KQTH >= b.dinhmuc_3 then 0.85	--11
																				when KHDK < b.dinhmuc_2 and KHDK >= b.dinhmuc_3 and KQTH < b.dinhmuc_3 then 0.85	--9
																				when KHDK < b.dinhmuc_3 and KQTH >= b.dinhmuc_2 then 0.9		--10
																				when KHDK < b.dinhmuc_2 and KHDK >= b.dinhmuc_3 and KQTH < b.dinhmuc_2 and KQTH >= b.dinhmuc_3 then 0.9		--8
																				when KHDK < b.dinhmuc_1  and KHDK >= b.dinhmuc_2 and KQTH < b.dinhmuc_3 then 0.9		--6
																				when KHDK < b.dinhmuc_2 and KHDK >= b.dinhmuc_3 and KQTH >= b.dinhmuc_2 then 0.95		--7
																				when KHDK < b.dinhmuc_1 and KHDK >= b.dinhmuc_2 and KQTH < b.dinhmuc_2 and KQTH >= b.dinhmuc_3 then 0.95	--5
																				when KHDK >= b.dinhmuc_1 and KQTH < b.dinhmuc_2 then 0.95		--3
																				when KHDK < b.dinhmuc_1 and KHDK >= b.dinhmuc_2 and KQTH >= b.dinhmuc_2 then 1  --4
																				when KHDK >= b.dinhmuc_1 and KQTH < b.dinhmuc_1 and KQTH >= b.dinhmuc_2 then 1		--2
																				when KHDK >= b.dinhmuc_1 and KQTH >= b.dinhmuc_1 then 1.1		--1
																	else null
																				 end
--					select * from ttkd_bsc.dinhmuc_giao_dthu_ptm a
					where a.ma_vtcv in ('VNP-HNHCM_BHKV_27') and a.thang = 202409 
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
					where ma_vtcv not in ('VNP-HNHCM_BHKV_15', 'VNP-HNHCM_BHKV_22', 'VNP-HNHCM_BHKV_41', 'VNP-HNHCM_BHKV_53', 'VNP-HNHCM_BHKV_6') and thang = 202409 
								and ma_pb not in (
																'VNP0702300',
																'VNP0702400',
																'VNP0702500'
																) 
;
		---Ap dung vb 292 dv BHDN eO 552546
		update ttkd_bsc.dinhmuc_giao_dthu_ptm a 
						set HESO_QD_DT_PTM = (select 
																		case when nvl(TYLE_THUCHIEN, 0) < 80 then 0.8
																				when TYLE_THUCHIEN >= 80 and TYLE_THUCHIEN < 90 then 0.85
																				when TYLE_THUCHIEN >= 90 and TYLE_THUCHIEN < 95 then 0.9
																				when TYLE_THUCHIEN >= 95 and TYLE_THUCHIEN < 98 then 0.95
																				when TYLE_THUCHIEN >= 98 and TYLE_THUCHIEN < 100 then 1
																				when TYLE_THUCHIEN >= 100 then 1.1 else null end heso_qd_dthu
																from ttkd_bsc.bangluong_kpi 
																where ma_kpi in ('HCM_DT_LUYKE_002') and thang = a.thang and ma_nv = a.ma_nv
															)
--			select * from ttkd_bsc.dinhmuc_giao_dthu_ptm a 															
			where thang = 202409 and
							ma_pb in (
											'VNP0702300',
											'VNP0702400',
											'VNP0702500'
											) 
			;
	
	rollback;
	commit;
------------------------------------------------------------------------------------------------

select *
		from admin.v_hs_thuebao a
					join admin.v_file_hs b on a.FILE_ID = b.FILE_ID
					;

		
		


