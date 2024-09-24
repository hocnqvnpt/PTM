/*
create table bangluong_kpi_202406_l6 as select * from bangluong_kpi_202406;  


ktra sau khi add du lieu thang do Xuan Vinh cung cap
select * from ttkd_bsc.dinhmuc_dthu_ptm where  thang=202406 ;
update ttkd_bsc.dinhmuc_dthu_ptm set  thang=202406 where thang is null;
update ttkd_bsc.dinhmuc_dthu_ptm set dt_giao_bsc='' where thang=202406 and dt_giao_bsc=0;
*/
create table ttkd_bsc.bangluong_kpi_202406_dot1 as select * from ttkd_bsc.bangluong_kpi_202406;  
create table ttkd_bsc.bangluong_kpi_dot1_20240725 as select * from ttkd_bsc.bangluong_kpi;  


select distinct a.*, b.*, c.ten_vtcv from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien c
        where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv and c.thang= 202406
                    and a.thang_kt is null and b.thang_kt is null 
                    and a.ma_kpi='HCM_DT_PTMOI_021' ;
    
                    

drop table ttkd_bsc.bangluong_kpi_202406_ptm purge
	;
		create table ttkd_bsc.bangluong_kpi_202406_ptm as 
			 select ma_nv, ten_nv , ma_vtcv,ten_vtcv,ma_donvi, ten_donvi , ma_to ,ten_to
						, hcm_dt_ptmoi_021_vnptt, hcm_dt_ptmoi_021_vnpts, hcm_dt_ptmoi_021_cntt, hcm_dt_ptmoi_021_cdbr_mytv, hcm_dt_ptmoi_021_khac, hcm_dt_ptmoi_021, hcm_dt_ptmoi_044
			 from ttkd_bsc.bangluong_kpi_202406
			;


		alter table ttkd_bsc.bangluong_kpi_202406_ptm 
			add (giao number, tong number, hcm_dt_ptmoi_021_ldp number)
			;
          
          
		drop table ttkd_bsc.temp_trasau_canhan purge;
		desc ttkd_bsc.ghtt_vnpts;
		;
		select * from ttkd_bsc.temp_trasau_canhan where ma_nv = 'VNP001752';
		----Tat ca dich vu, ngoai tru VNPtt
		create table ttkd_bsc.temp_trasau_canhan as
--		insert into ttkd_bsc.temp_trasau_canhan --(MANV_PTM, DTHU_KPI)
				select DICH_VU, LOAITB_ID, dichvuvt_id, cast(manv_ptm as varchar(20)) ma_nv,  round(sum(dthu_kpi)/1000000,3) dthu_kpi 
				from (
						---dich vu ngoai ctr OR dvu khac VNPtt
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_ptm, doanhthu_kpi_nvptm dthu_kpi
--									, dthu_goi * heso_dichvu * heso_tratruoc dthu_430
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi = 202406 and (loaitb_id<>21 or ma_kh='GTGT rieng')
										and doanhthu_kpi_nvptm >0
					union all
						---DNHM cho cot NVPTM
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_ptm, doanhthu_kpi_dnhm * heso_hotro_nvptm doanhthu_kpi_dnhm
--									, doanhthu_dnhm * heso_dichvu_dnhm * heso_tratruoc dthu_430
--									, (nvl(tien_dnhm,0)+nvl(tien_sodep,0)) *nvl(tyle_huongdt,1) *heso_dichvu_dnhm
						from ttkd_bsc.ct_bsc_ptm
						where thang_tlkpi_dnhm = 202406 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_dnhm >0
					union all
					---DNHM cho cot NVHOTRO
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_hotro, doanhthu_kpi_dnhm * heso_hotro_nvhotro doanhthu_kpi_dnhm
						from ttkd_bsc.ct_bsc_ptm
						where thang_tlkpi_dnhm = 202406 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_dnhm >0
					union all
					----Dthu nvho tro ban kenh ngoai
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_hotro, doanhthu_kpi_nvhotro 
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi_hotro = 202406
										and tyle_am is null and tyle_hotro is null and (loaitb_id<>21 or ma_kh='GTGT rieng')
										and doanhthu_kpi_nvhotro >0
					union all
					---Dthu nvhotro PGP
						select cast(thang_ptm as number) thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_hotro, doanhthu_kpi_nvhotro
						from ttkd_bsc.ct_bsc_ptm_pgp 
						where thang_tlkpi_hotro=202406 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_nvhotro >0
					union all
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_tt_dai, doanhthu_kpi_nvdai 
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi = 202406 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_nvdai >0
					union all
						----Gia han VNPts
						select thang, ma_kh, ma_tb, 'VNPTS' dich_vu, 20 loaitb_id, 2 dichvuvt_id, ma_nv, doanhthu_dongia 
						from ttkd_bsc.ghtt_vnpts 		--- Nguyen quan ly, chua co vb, toan noi mieng
						where thang=202406 and thang_giao is not null and ma_nv is not null
				)
--				where MANV_PTM in ('VNP017190','VNP027259')
				group by DICH_VU, LOAITB_ID, dichvuvt_id, manv_ptm
		;
	create index ttkd_bsc.temp_trasau_canhan_manv on ttkd_bsc.temp_trasau_canhan (ma_nv)
	;

-- ca nhan
		update ttkd_bsc.bangluong_kpi_202406_ptm a set hcm_dt_ptmoi_021_vnpts ='', hcm_dt_ptmoi_021_cdbr_mytv='', hcm_dt_ptmoi_021_cntt =''
		; 
		update ttkd_bsc.bangluong_kpi_202406_ptm a
		   set hcm_dt_ptmoi_021_vnpts = (select sum(dthu_kpi) from ttkd_bsc.temp_trasau_canhan  where ma_nv = a.ma_nv and loaitb_id = 20)
				 , hcm_dt_ptmoi_021_cdbr_mytv = 
--				 case when ma_donvi ='VNP0702600' 
--																						then (select sum(dthu_kpi) from ttkd_bsc.temp_trasau_canhan  
--																										where ma_nv = a.ma_nv and dichvuvt_id not in (2, 14, 15, 16))
--																					else 
																					(select sum(dthu_kpi)
																								from ttkd_bsc.temp_trasau_canhan a1
																										where ma_nv = a.ma_nv 
																													and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap1 in ('Mega+Fiber', 'MyTV') and loaitb_id = a1.loaitb_id)
																								)
--																					end
				, hcm_dt_ptmoi_021_cntt = 
--				case when ma_donvi ='VNP0702600' 
--																						then (select sum(dthu_kpi) from ttkd_bsc.temp_trasau_canhan  
--																										where ma_nv = a.ma_nv and dichvuvt_id in (14, 15, 16))
--																	else 
																	(select sum(dthu_kpi)
																			from ttkd_bsc.temp_trasau_canhan a1
																					where ma_nv = a.ma_nv 
																								and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id = a1.loaitb_id)
																			)
--																	end
				, hcm_dt_ptmoi_021_khac = (select sum(dthu_kpi) from ttkd_bsc.temp_trasau_canhan a1
																										where ma_nv = a.ma_nv
																										and not loaitb_id = 20
																										and not exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap1 in ('Mega+Fiber', 'MyTV') and loaitb_id = a1.loaitb_id)
																										and not exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id = a1.loaitb_id)
																)
			where exists (
									select 1 from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
										where ma_kpi='HCM_DT_PTMOI_021' and thang_kt is null and to_truong_pho is null and GIAMDOC_PHOGIAMDOC is null and ma_vtcv=a.ma_vtcv)
					
			;
		commit;							 

--		update ttkd_bsc.bangluong_kpi_202406_ptm a set tong='', hcm_dt_ptmoi_021=''
--		;
--		update ttkd_bsc.bangluong_kpi_202406_ptm a
--				set tong = nvl(hcm_dt_ptmoi_021_ts,0) + nvl(hcm_dt_ptmoi_021_tt,0)
--						, hcm_dt_ptmoi_021=nvl(hcm_dt_ptmoi_021_ts,0) + nvl(hcm_dt_ptmoi_021_tt,0)
--			-- select ma_nv, ma_vtcv, ten_vtcv, ten_donvi, hcm_dt_ptmoi_021_ts, hcm_dt_ptmoi_021_tt, tong, hcm_dt_ptmoi_021 from ttkd_bsc.bangluong_kpi_202406_ptm a
--			where (hcm_dt_ptmoi_021_ts is not null or hcm_dt_ptmoi_021_tt is not null)
----			and  ma_nv in ('VNP017190','VNP027259')
--		; 
 

-- to truong: thieu 0021_ts
	drop table ttkd_bsc.temp_totruong purge;
	
	select * from ttkd_bsc.temp_totruong;
	;
	create table ttkd_bsc.temp_totruong as
			select ma_pb, ma_to, loaitb_id, dichvuvt_id, round(sum(doanhthu_kpi_to)/1000000,3) dthu_kpi
			from(
					select ma_pb, ma_to, loaitb_id, dichvuvt_id, doanhthu_kpi_to
					  from ttkd_bsc.ct_bsc_ptm a
					  where thang_tlkpi_to=202406 and (loaitb_id<>21 or loaitb_id is null)
									and doanhthu_kpi_to >0
				union all
					---To  Nvien Cot MANV_PTM cho dthu DNHM
					select ma_pb, ma_to, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm * heso_hotro_nvptm as doanhthu_kpi_dnhm
					  from ttkd_bsc.ct_bsc_ptm a
					 where thang_tlkpi_dnhm_to=202406 and (loaitb_id<>21 or loaitb_id is null)
									and doanhthu_kpi_dnhm >0
				union all
					---To  Nvien Cot MANV_HOTRO cho dthu DNHM
					select b.ma_pb, b.ma_to, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm * heso_hotro_nvhotro as doanhthu_kpi_dnhm
					  from ttkd_bsc.ct_bsc_ptm a
								join ttkd_bsc.nhanvien b on a.thang_tlkpi_dnhm_to = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_dnhm_to=202406 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null
									and doanhthu_kpi_dnhm >0
				union all
					---To  Nvien Cot MANV_HOTRO PGP
					select b.ma_pb, b.ma_to, loaitb_id, dichvuvt_id, doanhthu_kpi_nvhotro
					  from ttkd_bsc.ct_bsc_ptm a
									join ttkd_bsc.nhanvien b on a.thang_tlkpi_hotro = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_hotro = 202406 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is not null and tyle_hotro is not null 
									and doanhthu_kpi_nvhotro >0
--				 ---MANV_DAI
					union all
						select b.ma_pb, b.ma_to, loaitb_id, dichvuvt_id, doanhthu_kpi_nvdai 
						from ttkd_bsc.ct_bsc_ptm a
										join ttkd_bsc.nhanvien b on a.thang_tlkpi = b.thang and a.manv_tt_dai = b.ma_nv
						where thang_tlkpi=202406 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_nvdai >0
				union all
					select ma_pb, ma_to, 20 loaitb_id, dichvuvt_id, doanhthu_dongia 
					from ttkd_bsc.ghtt_vnpts		----a Nguyen quan ly
					  where thang=202406 and thang_giao is not null and ma_to is not null 
					  
						)group by ma_pb, ma_to, loaitb_id, dichvuvt_id
		;

						 
		update ttkd_bsc.bangluong_kpi_202406_ptm a
		   set hcm_dt_ptmoi_021_vnpts=(select sum(dthu_kpi) from ttkd_bsc.temp_totruong where ma_to = a.ma_to and loaitb_id = 20)
					, hcm_dt_ptmoi_021_cdbr_mytv = (select sum(dthu_kpi)
																				from ttkd_bsc.temp_totruong a1
																						where exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap1 in ('Mega+Fiber', 'MyTV') and loaitb_id = a1.loaitb_id)
																									and ma_to = a.ma_to 
																				) 
				, hcm_dt_ptmoi_021_cntt = (select sum(dthu_kpi)
																			from ttkd_bsc.temp_totruong a1
																					where exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id = a1.loaitb_id)
																								and ma_to = a.ma_to
																			)
				, hcm_dt_ptmoi_021_khac = (select sum(dthu_kpi) from ttkd_bsc.temp_totruong a1
																										where ma_to = a.ma_to
																										and not loaitb_id = 20
																										and not exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap1 in ('Mega+Fiber', 'MyTV') and loaitb_id = a1.loaitb_id)
																										and not exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id = a1.loaitb_id)
																)
--		  select ma_nv, ma_to, ten_donvi, ten_vtcv, hcm_dt_ptmoi_021_ts from bangluong_kpi_202406_ptm a
			 where exists (select dthu_kpi from ttkd_bsc.temp_totruong where ma_to=a.ma_to) 
				and exists(select 1 from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
										where ma_kpi='HCM_DT_PTMOI_021' and thang_kt is null and to_truong_pho is not null and ma_vtcv=a.ma_vtcv);    
		;
		
--		update ttkd_bsc.bangluong_kpi_202406_ptm a
--				set tong = nvl(hcm_dt_ptmoi_021_ts,0) + nvl(hcm_dt_ptmoi_021_tt,0)
--					   ,hcm_dt_ptmoi_021 = nvl(hcm_dt_ptmoi_021_ts,0) + nvl(hcm_dt_ptmoi_021_tt,0)
--			-- select ma_nv, ma_vtcv, ten_vtcv, ten_donvi, hcm_dt_ptmoi_021_ts, hcm_dt_ptmoi_021_tt, tong, hcm_dt_ptmoi_021 from ttkd_bsc.bangluong_kpi_202406_ptm a
--			where (hcm_dt_ptmoi_021_ts is not null or hcm_dt_ptmoi_021_tt is not null)
		; 
commit;
---- ldp phu trach: tinh theo MA_TO va NHOM dich vu
		
		select * from ttkd_bsc.temp_021_ldp1;
		select * from ttkd_bsc.temp_021_ldp;
		 select sum(DTHU_KPI) from ttkd_bsc.temp_021_ldp1; group by dv_cap2; where dv_cap2 in ('Dich vu so doanh nghiep');1608.982
		 select sum(DTHU_KPI) from ttkd_bsc.temp_021_ldp; where dv_cap2 in ('Dich vu so doanh nghiep'); 8836.685 
		 
		 drop table  ttkd_bsc.temp_021_ldp1 purge;
	 create table ttkd_bsc.temp_021_ldp1 as
				select a.ma_pb, a.ma_to, b.dv_cap1, b.dv_cap2, sum(dthu_kpi) dthu_kpi
									from ttkd_bsc.temp_totruong a
											left join ttkd_bsc.dm_loaihinh_hsqd b on a.loaitb_id = b.loaitb_id 
																and (dv_cap2 in ('Dich vu so doanh nghiep','Truyen so lieu', 'Internet truc tiep') or dv_cap1 in ('VNP tra sau', 'VNP tra truoc', 'Mega+Fiber', 'MyTV'))
											group by a.ma_pb, a.ma_to, b.dv_cap1, b.dv_cap2
			;
		select * from	ttkd_bsc.blkpi_dm_to_pgd pgd
							where pgd.thang=202406 
										and lower(pgd.ma_kpi) in ('hcm_dt_ptmoi_021') and ma_pb in ('VNP0702300', 'VNP0702400', 'VNP0702500')
										;
			drop table  ttkd_bsc.temp_021_ldp purge;
	create table ttkd_bsc.temp_021_ldp as	
			with pgd as ( select distinct MA_PB, MA_TO, MA_NV, THANG, MA_KPI, DV_CAP1, DV_CAP2 
									from ttkd_bsc.blkpi_dm_to_pgd pgd 
									where thang = 202406 and ma_kpi = 'HCM_DT_PTMOI_021'
									)
--			select a.*, pgd.ma_nv 
--			from ttkd_bsc.temp_021_ldp1 a
--						join pgd
--							on a.ma_to = pgd.ma_to and pgd.thang=202406
--			where a.ma_pb in ('VNP0702300', 'VNP0702400', 'VNP0702500')			--BHDN theo To
--			union all
						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									 join pgd
										on a.ma_to = pgd.ma_to and a.dv_cap1 = pgd.dv_cap1  and pgd.thang=202406 
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')		---BHKV theo To va Dich vu VNP tra sau
									and a.dv_cap1 = 'VNP tra sau'
						union all
						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									 join pgd
										on a.ma_to = pgd.ma_to and a.dv_cap1 = pgd.dv_cap1  and pgd.thang=202406
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')		---BHKV theo To va Dich vu 'Mega, Fiber', 'MyTV'
										and a.dv_cap1 in ('Mega+Fiber', 'MyTV')
						union all
						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									 join pgd
										on a.ma_to = pgd.ma_to and a.dv_cap2 = pgd.dv_cap2 and pgd.thang=202406
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')		---BHKV theo To va Dich vu Dich vu so doanh nghiep
										and a.dv_cap2 in ('Dich vu so doanh nghiep')
						union all
						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									 join pgd on a.ma_to = pgd.ma_to and nvl(a.dv_cap2, 'Truyen so lieu') = pgd.dv_cap2 
															and pgd.thang=202406
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')		---BHKV theo To va Dich vu TSL, INT, con lai
									and not nvl(a.dv_cap2, 'a') in ('Dich vu so doanh nghiep')
									and not nvl(a.dv_cap1, 'a') in ('Mega+Fiber', 'MyTV')
									and not nvl(a.dv_cap1, 'a') = 'VNP tra sau'
									and nvl(a.dv_cap2, 'Truyen so lieu') in ('Truyen so lieu', 'Internet truc tiep')
			;
			---KIEM tra bang sau khi chay
					select sum(dthu_kpi) from
						(
						with pgd as ( select distinct MA_PB, MA_TO, MA_NV, THANG, MA_KPI, DV_CAP1, DV_CAP2 
															from ttkd_bsc.blkpi_dm_to_pgd pgd 
															where thang = 202406 and ma_kpi = 'HCM_DT_PTMOI_021'
															)
									select a.*, pgd.ma_nv
													, case when exists (select 1 from pgd where ma_pb = a.ma_pb) then 1 else 0 end exit_ma_pb
													, case when exists (select 1 from pgd where ma_to = a.ma_to) then 1 else 0 end exit_ma_to
									from ttkd_bsc.temp_021_ldp1 a
												left join pgd on a.ma_to = pgd.ma_to and (a.DV_CAP1 = pgd.DV_CAP1 or nvl(a.DV_CAP2, 'Truyen so lieu') = pgd.DV_CAP2) and pgd.thang=202406
						) where exit_ma_pb + exit_ma_to = 2 and ma_nv is not null
		;
		
		update ttkd_bsc.bangluong_kpi_202406_ptm a
		   set hcm_dt_ptmoi_021_vnpts = (select sum(dthu_kpi) from ttkd_bsc.temp_021_ldp where dv_cap1 = 'VNP tra sau' and ma_nv = a.ma_nv)
				, hcm_dt_ptmoi_021_cdbr_mytv = (select sum(dthu_kpi) from ttkd_bsc.temp_021_ldp a1 where dv_cap1 in ('Mega+Fiber', 'MyTV') and ma_nv = a.ma_nv)
				, hcm_dt_ptmoi_021_cntt = (select sum(dthu_kpi) from ttkd_bsc.temp_021_ldp a1 where dv_cap2 in ('Dich vu so doanh nghiep') and ma_nv = a.ma_nv)
				, hcm_dt_ptmoi_021_khac = (select sum(dthu_kpi) from ttkd_bsc.temp_021_ldp a1 
																		where  not nvl(a1.dv_cap2, 'a') in ('Dich vu so doanh nghiep')
																						and not nvl(a1.dv_cap1, 'a') in ('Mega+Fiber', 'MyTV')
																						and not nvl(a1.dv_cap1, 'a') = 'VNP tra sau'
																						and nvl(a1.dv_cap2, 'Truyen so lieu') in ('Truyen so lieu', 'Internet truc tiep')
																						and ma_nv = a.ma_nv)
		where  exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi='HCM_DT_PTMOI_021' and thang_kt is null and giamdoc_phogiamdoc is not null and ma_vtcv=a.ma_vtcv)
					and exists(select * from ttkd_bsc.temp_021_ldp where ma_nv = a.ma_nv)
					;      
		select * from ttkd_bsc.bangluong_kpi_202406_ptm;
-- -_da xong

commit;
	


-- update bangluong_kpi_202406:    
		create table ttkd_bsc.bangluong_kpi_202406_l2 as select * from ttkd_bsc.bangluong_kpi_202406 a;
		update ttkd_bsc.bangluong_kpi_202406 a 
				set HCM_DT_PTMOI_021_TS = '' 
			where  hcm_dt_ptmoi_021 is not null 
			;    
			
		update ttkd_bsc.bangluong_kpi_202406 a 
				set HCM_DT_PTMOI_021_VNPTS = (select HCM_DT_PTMOI_021_VNPTS 
																						from ttkd_bsc.bangluong_kpi_202406_ptm where ma_nv=a.ma_nv)
						, hcm_dt_ptmoi_021_cdbr_mytv = (select hcm_dt_ptmoi_021_cdbr_mytv
																						from ttkd_bsc.bangluong_kpi_202406_ptm where ma_nv=a.ma_nv)
						, hcm_dt_ptmoi_021_cntt = (select hcm_dt_ptmoi_021_cntt
																						from ttkd_bsc.bangluong_kpi_202406_ptm where ma_nv=a.ma_nv)
						, hcm_dt_ptmoi_021_khac = (select hcm_dt_ptmoi_021_khac
																						from ttkd_bsc.bangluong_kpi_202406_ptm where ma_nv=a.ma_nv)
				
			--where exists(select 1 from ttkd_bsc.bangluong_kpi_202406_ptm where HCM_DT_PTMOI_021_TS is not null and ma_nv=a.ma_nv) 
			;
		update ttkd_bsc.bangluong_kpi_202406 a 
					set HCM_DT_PTMOI_021 = nvl(hcm_dt_ptmoi_021_vnptt, 0) + nvl(hcm_dt_ptmoi_021_vnpts, 0) 
																	+ nvl(hcm_dt_ptmoi_021_cdbr_mytv, 0) + nvl(hcm_dt_ptmoi_021_cntt, 0) + nvl(hcm_dt_ptmoi_021_khac, 0)
			;
		
		update ttkd_bsc.bangluong_kpi_202406 a set hcm_dt_ptmoi_021='' where hcm_dt_ptmoi_021=0; 
		update ttkd_bsc.bangluong_kpi_202406 a set hcm_dt_ptmoi_021_vnptt='' where hcm_dt_ptmoi_021_vnptt=0;
		update ttkd_bsc.bangluong_kpi_202406 a set hcm_dt_ptmoi_021_vnpts='' where hcm_dt_ptmoi_021_vnpts=0; 
		update ttkd_bsc.bangluong_kpi_202406 a set hcm_dt_ptmoi_021_cdbr_mytv='' where hcm_dt_ptmoi_021_cdbr_mytv=0;
		update ttkd_bsc.bangluong_kpi_202406 a set hcm_dt_ptmoi_021_cntt='' where hcm_dt_ptmoi_021_cntt=0;
			
		commit;
		
		select * from ttkd_bsc.dinhmuc_giao_dthu_ptm;
		select ma_nv, hcm_dt_ptmoi_021_vnptt, hcm_dt_ptmoi_021_vnpts, hcm_dt_ptmoi_021_cdbr_mytv, hcm_dt_ptmoi_021_cntt, HCM_DT_PTMOI_021 from ttkd_bsc.bangluong_kpi_202406 a;
		select * from ttkd_bsc.bangluong_kpi a where ma_kpi in ('HCM_DT_PTMOI_021') and thang = 202406;
		
		update ttkd_bsc.bangluong_kpi a set THUCHIEN = (select HCM_DT_PTMOI_021 from ttkd_bsc.bangluong_kpi_202406 where ma_nv = a.ma_nv)
				where ma_kpi in ('HCM_DT_PTMOI_021') and thang = 202406
				;
		update ttkd_bsc.bangluong_kpi a set GIAO = (select round(TONG_DTGIAO/1000000, 3) from ttkd_bsc.dinhmuc_giao_dthu_ptm 
																						where thang = a.thang and ma_nv =a.ma_nv 
																						and trunc(dateinput) = '28/07/2024')
				where a.ma_kpi in ('HCM_DT_PTMOI_021') and a.thang = 202406
			;
		update ttkd_bsc.bangluong_kpi a set TYLE_THUCHIEN = case when GIAO = 0 then null 
																														else ROUND(THUCHIEN/GIAO*100, 2) end
				where ma_kpi in ('HCM_DT_PTMOI_021') and thang = 202406
			;
		update ttkd_bsc.bangluong_kpi_202406 a set HCM_DT_PTMOI_021_giao = (select GIAO from ttkd_bsc.bangluong_kpi a 
																																		where ma_kpi in ('HCM_DT_PTMOI_021') and thang = 202406 and a.ma_nv = b.ma_nv)
		;
/*
select distinct a.*, c.ten_vtcv
                ,(select count(*) from bangluong_kpi_202406 where ma_vtcv=b.ma_vtcv ) sl_nv
                ,(select count(*) from bangluong_kpi_202406 where hcm_dt_ptmoi_021 is not null and ma_vtcv=b.ma_vtcv ) slnv_dadanhgia
        from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien_202406 c  
        where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv 
                   and a.thang_kt is null and b.thang_kt is null 
                    and lower(a.ma_kpi)='hcm_dt_ptmoi_021' ;
 
                    
select a.ten_donvi, a.ma_to, a.ten_to, a.ma_nv_hrm,  a.ten_nv, a.ma_vtcv, a.ten_vtcv
             ,a.hcm_dt_ptmoi_021 hcm_dt_ptmoi_021_new, b.hcm_dt_ptmoi_021 hcm_dt_ptmoi_021_old,
             nvl(a.hcm_dt_ptmoi_021,0) - nvl(b.hcm_dt_ptmoi_021,0) chechlech
  from bangluong_kpi_202406 a, bangluong_kpi_202406_l5 b 
where a.ma_nv=b.ma_nv 
            and nvl(a.hcm_dt_ptmoi_021,0)<>nvl(b.hcm_dt_ptmoi_021,0)   
order by (a.hcm_dt_ptmoi_021 - b.hcm_dt_ptmoi_021);
                    

*/

------------------------------------------------------------------------------------------------


--- DTHU CNTT:
/*-- Kiem tra danh muc loaitb_id thieu trong danh muc
insert into ttkd_bsc.dm_loaihinh_hsqd (DICHVUVT_ID, LOAITB_ID, LOAIHINH_TB)
    select distinct dichvuvt_id,loaitb_id, dich_vu --, (select loaihinh_tb from css_hcm.loaihinh_tb@ttkddb where loaitb_id=a.loaitb_id)loaihinh_tb
    from ttkd_bsc.ct_bsc_ptm a
    where thang_ptm=202406
        and not exists(select * from ttkd_bsc.dm_loaihinh_hsqd where loaitb_id=a.loaitb_id);
        
select distinct b.loaitb_id, b.loaihinh_tb, b.dv_cap2
  from ct_bsc_ptm a, ttkd_bsc.dm_loaihinh_hsqd b
    where b.loaitb_id<>21 and a.loaitb_id=b.loaitb_id and a.thang_tlkpi=202406 and b.dv_cap2 is null;

*/
	
	select distinct a.*, b.*, c.ten_vtcv from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien c
        where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv and c.thang= 202406
                    and a.thang_kt is null and b.thang_kt is null 
                    and a.ma_kpi='HCM_DT_PTMOI_044' ;
				
	select HCM_DT_PTMOI_044 
	from ttkd_bsc.bangluong_kpi_202406 a
    where HCM_DT_PTMOI_044 is not null
			and exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
						where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and giamdoc_phogiamdoc is not null
						  and ma_vtcv=a.ma_vtcv
						 )
					  ;
                      
                      
--TT = tat ca NV trong to khong loai tru thu viec hay CTV
		select loaitb_id from ttkd_bsc.ct_bsc_ptm a
		where thang_tlkpi=202406 and dichvuvt_id in (13,14,15,16) 
				and exists(select * from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 is null and loaitb_id=a.loaitb_id)
				;


----------- Ca nhan 
			--- chi tieu nay ap dung DICH VU SO DOANH NGHIEP, khong ap dung PGP, va nvhotro kenh ngoai khong co ban
		drop table ttkd_bsc.temp_cntt purge;
		;
		
--		select * from ttkd_bsc.temp_cntt;
		create table ttkd_bsc.temp_cntt as
				select manv_ptm ma_nv,sum(doanhthu_kpi_nvptm)dthu
				  from(
						select manv_ptm, doanhthu_kpi_nvptm
						  from ttkd_bsc.ct_bsc_ptm a
								where thang_tlkpi=202406
											  and (exists(select * from ttkd_bsc.dm_loaihinh_hsqd 
																		where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id=a.loaitb_id)
															or loaitb_id = 999
													)
					union all
						select manv_ptm, (doanhthu_kpi_dnhm * heso_hotro_nvptm) as doanhthu_kpi_dnhm
						  from ttkd_bsc.ct_bsc_ptm a
						 where thang_tlkpi_dnhm=202406
						   and (exists (select * from ttkd_bsc.dm_loaihinh_hsqd 
																		where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id=a.loaitb_id)
															or loaitb_id=999
													)
				)group by manv_ptm
		;
 
-- Kiem tra:
		select distinct loaitb_id from ttkd_bsc.ct_bsc_ptm where thang_tlkpi=202406; and loaitb_id is null
		;

		update ttkd_bsc.bangluong_kpi_202406 a
		   set HCM_DT_PTMOI_044 = (select round(dthu/1000000,3) from ttkd_bsc.temp_cntt where ma_nv = a.ma_nv) 
		 where exists(select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
					   where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and to_truong_pho is null and giamdoc_phogiamdoc is null
						 and ma_vtcv=a.ma_vtcv)
			;


		select a.ma_nv, a.ten_vtcv, a.ten_donvi, HCM_DT_PTMOI_044 from ttkd_bsc.bangluong_kpi_202406 a
		 where exists(select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
					   where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and to_truong_pho is null and giamdoc_phogiamdoc is null
						 and ma_vtcv=a.ma_vtcv)
			;
            
      commit;
----------- TT            bo to dai ly
			drop table ttkd_bsc.temp_cntt purge;
			create table ttkd_bsc.temp_cntt as
			select ma_to,sum(doanhthu_kpi_to)dthu 
			from(
							select ma_to, doanhthu_kpi_to
							  from ttkd_bsc.ct_bsc_ptm a
							 where thang_tlkpi_to = 202406
							   and (exists(select * from ttkd_bsc.dm_loaihinh_hsqd 
																		where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id=a.loaitb_id)
															or loaitb_id=999
													)
							 union all
							select ma_to, doanhthu_kpi_dnhm_phong
							  from ttkd_bsc.ct_bsc_ptm a
							 where thang_tlkpi_dnhm_to = 202406
							   and (exists(select * from ttkd_bsc.dm_loaihinh_hsqd 
																		where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id=a.loaitb_id)
															or loaitb_id=999
													)
			)group by ma_to
			;
			 
			update ttkd_bsc.bangluong_kpi_202406 a
			   set HCM_DT_PTMOI_044 = (select round(dthu/1000000,3) from ttkd_bsc.temp_cntt where ma_to=a.ma_to) 
			 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
							where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and to_truong_pho is not null
							  and ma_vtcv=a.ma_vtcv)
							  ;
							  commit;

			select a.ma_nv, a.ten_vtcv, a.ten_donvi, HCM_DT_PTMOI_044 
			from  ttkd_bsc.bangluong_kpi_202406 a
			 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
							where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and to_truong_pho is not null
							  and ma_vtcv=a.ma_vtcv)
							  ;
							  
                                   
----------- PGD
		select * from ttkd_bsc.temp_cntt;
		drop table ttkd_bsc.temp_cntt purge;
		create table ttkd_bsc.temp_cntt as
				select manv_pgd,dthu 
				  from (select ma_to,sum(doanhthu_kpi_phong)dthu
						  from (select ma_to,doanhthu_kpi_phong     
											  from ttkd_bsc.ct_bsc_ptm a
											 where thang_tlkpi_phong = 202406
														   and (exists(select * from ttkd_bsc.dm_loaihinh_hsqd 
																								where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id=a.loaitb_id)
																					or loaitb_id=999
																			)
										
											 union all
												select ma_to, doanhthu_kpi_dnhm_phong
														  from ttkd_bsc.ct_bsc_ptm a
														 where thang_tlkpi_dnhm_phong = 202406
																	  and (exists(select * from ttkd_bsc.dm_loaihinh_hsqd 
																											where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id=a.loaitb_id)
																								or loaitb_id=999
																						)
							   )
						  group by ma_to)x
					  ,(select ma_nv manv_pgd,ma_to, ma_kpi from ttkd_bsc.blkpi_dm_to_pgd where thang=202406 and upper(ma_kpi)='HCM_DT_PTMOI_044')y
				 where x.ma_to=y.ma_to
		;
		 
		 
		update ttkd_bsc.bangluong_kpi_202406 a
		   set HCM_DT_PTMOI_044=''
		 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
						where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and giamdoc_phogiamdoc is not null
						  and ma_vtcv=a.ma_vtcv);
			  
		update ttkd_bsc.bangluong_kpi_202406 a
		   set HCM_DT_PTMOI_044=(select round(sum(dthu)/1000000,3) from ttkd_bsc.temp_cntt where manv_pgd=a.ma_nv) 
		 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
						where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and giamdoc_phogiamdoc is not null
						  and ma_vtcv = a.ma_vtcv);
						  
		commit;
		
		select * from ttkd_bsc.bangluong_kpi a where thang = 202406 and ma_kpi = 'HCM_DT_PTMOI_044';
		
		update ttkd_bsc.bangluong_kpi a set thuchien = (select HCM_DT_PTMOI_044 from ttkd_bsc.bangluong_kpi_202406 where ma_nv = a.ma_nv)
				where thang = 202406 and ma_kpi = 'HCM_DT_PTMOI_044'
		;
---END HCM_DT_PTMOI_044

		 select distinct a.*, b.ma_vtcv, c.ten_vtcv
						,(select count(*) from ttkd_bsc.bangluong_kpi_202406 
								where hcm_dt_ptmoi_044 is not null and ma_vtcv=b.ma_vtcv ) slnv_dadanhgia
				from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien_202406 c  
				where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv 
						   and a.thang_kt is null and b.thang_kt is null 
							and lower(a.ma_kpi)='hcm_dt_ptmoi_044' ;
		 
		 
		select a.ten_donvi, a.ten_to, a.ma_nv_hrm,  a.ten_nv, a.ma_vtcv, a.ten_vtcv
					 ,a.hcm_dt_ptmoi_044 new, b.hcm_dt_ptmoi_044 old,
					 nvl(a.hcm_dt_ptmoi_044,0) - nvl(b.hcm_dt_ptmoi_044,0) chechlech
		  from ttkd_bsc.bangluong_kpi_202406 a, ttkd_bsc.bangluong_kpi_202406_l7 b 
		where a.ma_nv=b.ma_nv 
					and nvl(a.hcm_dt_ptmoi_044,0)<>nvl(b.hcm_dt_ptmoi_044,0)    
		order by (a.hcm_dt_ptmoi_044 - b.hcm_dt_ptmoi_044);


		-- san luong phát trien moi brcd, vnp tra sau
			---kiem tra co giao hay khong
		select distinct a.*, b.*, b.ten_vtcv from blkpi_danhmuc_kpi a, blkpi_danhmuc_kpi_vtcv b, nhanvien_202402 c  
				where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv       
							and a.thang_kt is null and b.thang_kt is null 
							and lower(a.ma_kpi)='hcm_sl_brvnp_001' 
			;




