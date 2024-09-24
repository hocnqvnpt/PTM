/*
create table bangluong_kpi_202407_l6 as select * from bangluong_kpi_202407;  


ktra sau khi add du lieu thang do Xuan Vinh cung cap
select * from ttkd_bsc.dinhmuc_dthu_ptm where  thang=202407 ;
update ttkd_bsc.dinhmuc_dthu_ptm set  thang=202407 where thang is null;
update ttkd_bsc.dinhmuc_dthu_ptm set dt_giao_bsc='' where thang=202407 and dt_giao_bsc=0;
*/
create table ttkd_bsc.bangluong_kpi_202407_dot2 as select * from ttkd_bsc.bangluong_kpi_202407;  
create table ttkd_bsc.bangluong_kpi_dot2_20240820 as select * from ttkd_bsc.bangluong_kpi where thang = 202407;  


select distinct a.*, b.*, c.ten_vtcv 
		from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien c
        where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv and c.thang= 202407
                    and a.thang = b.thang and a.thang = c.thang
                    and a.ma_kpi='HCM_DT_PTMOI_021' ;
    
                    

drop table ttkd_bsc.bangluong_kpi_202407_ptm purge
	;
		create table ttkd_bsc.bangluong_kpi_202407_ptm as 
			 select ma_nv, ten_nv , ma_vtcv,ten_vtcv,ma_donvi, ten_donvi , ma_to ,ten_to
						, hcm_dt_ptmoi_021_vnptt, hcm_dt_ptmoi_021_vnpts, hcm_dt_ptmoi_021_cntt, hcm_dt_ptmoi_021_cdbr_mytv, hcm_dt_ptmoi_021_khac, hcm_dt_ptmoi_021, hcm_dt_ptmoi_044
			 from ttkd_bsc.bangluong_kpi_202407
			;


--		alter table ttkd_bsc.bangluong_kpi_202407_ptm 
--			add (giao number, tong number, hcm_dt_ptmoi_021_ldp number)
--			;
--          
          
		drop table ttkd_bsc.temp_trasau_canhan purge;
		desc ttkd_bsc.ghtt_vnpts;
		;
		select * from ttkd_bsc.temp_trasau_canhan where ma_nv = 'CTV071620';
		----Tat ca dich vu, ngoai tru VNPtt
		create table ttkd_bsc.temp_trasau_canhan as;
--		insert into ttkd_bsc.temp_trasau_canhan --(MANV_PTM, DTHU_KPI)
				select DICH_VU, LOAITB_ID, dichvuvt_id, cast(manv_ptm as varchar(20)) ma_nv,  round(sum(dthu_kpi)/1000000,3) dthu_kpi 
				from (
					
						---DNHM cho cot NVPTM
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_ptm, doanhthu_kpi_dnhm * heso_hotro_nvptm dthu_kpi
						from ttkd_bsc.ct_bsc_ptm
						where thang_tlkpi_dnhm = 202407 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_dnhm >0
					union all
					---DNHM cho cot NVHOTRO kenh ngoai
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_hotro, doanhthu_kpi_dnhm * heso_hotro_nvhotro doanhthu_kpi_dnhm
						from ttkd_bsc.ct_bsc_ptm
						where thang_tlkpi_hotro = 202407 
									and tyle_am is null and tyle_hotro is null and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_dnhm >0
					union all
					---DNHM cho cot DAI
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_tt_dai, doanhthu_kpi_dnhm * heso_hotro_dai doanhthu_kpi_dnhm
						from ttkd_bsc.ct_bsc_ptm
						where thang_tldg_dt_dai = 202407 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_dnhm >0
					union all
						---dich vu ngoai ctr OR dvu khac VNPtt
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_ptm, doanhthu_kpi_nvptm dthu_kpi
--									, dthu_goi * heso_dichvu * heso_tratruoc dthu_430
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi = 202407 and (loaitb_id<>21 or ma_kh='GTGT rieng')
										and doanhthu_kpi_nvptm >0
					union all
					----Dthu nvho tro ban kenh ngoai
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_hotro, doanhthu_kpi_nvhotro 
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi_hotro = 202407
										and tyle_am is null and tyle_hotro is null and (loaitb_id<>21 or ma_kh='GTGT rieng')
										and doanhthu_kpi_nvhotro >0
					union all
					---Dthu nvhotro PGP
						select cast(thang_ptm as number) thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_hotro, doanhthu_kpi_nvhotro
						from ttkd_bsc.ct_bsc_ptm_pgp 
						where thang_tlkpi_hotro=202407 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_nvhotro >0
					union all
					---Dthu nv DAI
						select thang_ptm, ma_gd, ma_tb, dich_vu, loaitb_id, dichvuvt_id, manv_tt_dai, doanhthu_kpi_nvdai 
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tldg_dt_dai = 202407 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_nvdai >0
					union all
					----Gia han VNPts
						select thang, ma_kh, ma_tb, 'VNPTS' dich_vu, 20 loaitb_id, 2 dichvuvt_id, ma_nv, doanhthu_dongia 
						from ttkd_bsc.ghtt_vnpts 		--- Nguyen quan ly, chua co vb, toan noi mieng
						where thang=202407 and thang_giao is not null and ma_nv is not null
				)
--				where MANV_PTM in ('CTV071620','VNP027259')
				group by DICH_VU, LOAITB_ID, dichvuvt_id, manv_ptm
		;
	create index ttkd_bsc.temp_trasau_canhan_manv on ttkd_bsc.temp_trasau_canhan (ma_nv)
	;
-- to truong: thieu 0021_ts
	drop table ttkd_bsc.temp_totruong purge;
	
	select sum(dthu_kpi) dthu_to, 0 dthu_cn from ttkd_bsc.temp_totruong ;where ma_to = 'VNP0701680';9379.411
	union all select 0, sum(dthu_kpi) dthu_cn,ma_to from ttkd_bsc.temp_trasau_canhan;
	;
	create table ttkd_bsc.temp_totruong as
			select ma_pb, ma_to, loaitb_id, dichvuvt_id, round(sum(doanhthu_kpi_to)/1000000,3) dthu_kpi
			from(
					select ma_pb, ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_to
					  from ttkd_bsc.ct_bsc_ptm a
					  where thang_tlkpi_to = 202407 and (loaitb_id<>21 or loaitb_id is null)
									and doanhthu_kpi_to >0
				union all
					---To  Nvien Cot MANV_PTM cho dthu DNHM
					select ma_pb, ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm * heso_hotro_nvptm as doanhthu_kpi_dnhm
					  from ttkd_bsc.ct_bsc_ptm a
					 where thang_tlkpi_dnhm_to=202407 and (loaitb_id<>21 or loaitb_id is null)
									and doanhthu_kpi_dnhm >0
				union all
					---To  Nvien Cot MANV_HOTRO cho dthu DNHM
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_dnhm * heso_hotro_nvhotro as doanhthu_kpi_dnhm
					  from ttkd_bsc.ct_bsc_ptm a
								join ttkd_bsc.nhanvien b on a.thang_tlkpi_dnhm_to = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_dnhm_to=202407 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is null and tyle_hotro is null
									and doanhthu_kpi_dnhm >0
									and nvl(vanban_id, 0) != 764 ---only T7 xoa
				union all
					---To  Nvien Cot MANV_HOTRO PGP
					select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_nvhotro
					  from ttkd_bsc.ct_bsc_ptm a
									join ttkd_bsc.nhanvien b on a.thang_tlkpi_hotro = b.thang and a.manv_hotro = b.ma_nv
					 where thang_tlkpi_hotro = 202407 and (loaitb_id<>21 or loaitb_id is null)
									and tyle_am is not null and tyle_hotro is not null 
									and doanhthu_kpi_nvhotro >0
--				 ---MANV_DAI
					union all
						select b.ma_pb, b.ma_to, ma_tb, loaitb_id, dichvuvt_id, doanhthu_kpi_nvdai 
						from ttkd_bsc.ct_bsc_ptm a
										join ttkd_bsc.nhanvien b on a.thang_tlkpi = b.thang and a.manv_tt_dai = b.ma_nv
						where thang_tlkpi=202407 and (loaitb_id<>21 or ma_kh='GTGT rieng')
									and doanhthu_kpi_nvdai >0
									and nvl(vanban_id, 0) != 764  ---only T7 xoa
				union all
					select ma_pb, ma_to, ma_tb, 20 loaitb_id, 2 dichvuvt_id, doanhthu_dongia 
					from ttkd_bsc.ghtt_vnpts		----a Nguyen quan ly
					  where thang=202407 and thang_giao is not null and ma_to is not null 
					  
						)group by ma_pb, ma_to, loaitb_id, dichvuvt_id
		;

						 
		
---- ldp phu trach: tinh theo MA_TO va NHOM dich vu
		
		select * from ttkd_bsc.temp_021_ldp1;
		select * from ttkd_bsc.temp_021_ldp;
		 select sum(DTHU_KPI) from ttkd_bsc.temp_021_ldp1; 9379.411
		 select sum(DTHU_KPI) from ttkd_bsc.temp_021_ldp; 3644.041
		 
		 drop table  ttkd_bsc.temp_021_ldp1 purge
		 ;
		create table ttkd_bsc.temp_021_ldp1 as
				select a.ma_pb, a.ma_to, b.dv_cap1, b.dv_cap2
							, case when dv_cap1 in ('VNP tra sau', 'VNP tra truoc', 'Mega+Fiber', 'MyTV') then dv_cap1
										when dv_cap2 in ('Dich vu so doanh nghiep') then dv_cap2
									else 'Con lai' end dichvu
							, case when dv_cap1 in ('VNP tra sau', 'VNP tra truoc', 'Mega+Fiber', 'MyTV') then 'Dichvu_cap1'
										when dv_cap2 in ('Dich vu so doanh nghiep') then 'Dichvu_cap2'
									else 'Dichvu_Con lai' end nhom_dichvu
							, sum(dthu_kpi) dthu_kpi
									from ttkd_bsc.temp_totruong a
											left join ttkd_bsc.dm_loaihinh_hsqd b on a.loaitb_id = b.loaitb_id 
																and (dv_cap2 in ('Dich vu so doanh nghiep','Truyen so lieu', 'Internet truc tiep') or dv_cap1 in ('VNP tra sau', 'VNP tra truoc', 'Mega+Fiber', 'MyTV'))
											group by a.ma_pb, a.ma_to, b.dv_cap1, b.dv_cap2
			;
		select * from	ttkd_bsc.blkpi_dm_to_pgd pgd
							where pgd.thang=202407 
										and ma_pb in ('VNP0702300', 'VNP0702400', 'VNP0702500')
										;
										
			drop table  ttkd_bsc.temp_021_ldp purge;
	create table ttkd_bsc.temp_021_ldp as	
			with pgd as ( select distinct MA_PB, MA_TO, MA_NV, THANG, dichvu, nhom_dichvu
									from ttkd_bsc.blkpi_dm_to_pgd pgd 
									where thang = 202407 and nhom_dichvu is not null
									)

						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									 left join pgd
										on a.ma_to = pgd.ma_to and a.dichvu = pgd.dichvu and a.nhom_dichvu = pgd.nhom_dichvu  and pgd.thang=202407 
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')		---BHKV theo To va Dich vu VNP tra sau
									and a.dichvu = 'VNP tra sau'
						union all
						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									 join pgd
										on a.ma_to = pgd.ma_to and a.dichvu = pgd.dichvu and a.nhom_dichvu = pgd.nhom_dichvu  and pgd.thang=202407 
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')		---BHKV theo To va Dich vu 'Mega, Fiber', 'MyTV'
										and a.dichvu in ('Mega+Fiber', 'MyTV')
						union all
						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									 join pgd
										on a.ma_to = pgd.ma_to and a.dichvu = pgd.dichvu and a.nhom_dichvu = pgd.nhom_dichvu  and pgd.thang=202407 
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')		---BHKV theo To va Dich vu Dich vu so doanh nghiep
										and a.dichvu in ('Dich vu so doanh nghiep')
						union all
						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									  join pgd on a.ma_to = pgd.ma_to and a.dichvu = pgd.dichvu and a.nhom_dichvu = pgd.nhom_dichvu  and pgd.thang=202407 
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')		---BHKV theo To va Dich vu TSL, INT, con lai
--									and not nvl(a.dichvu, 'a') in ('Dich vu so doanh nghiep')
--									and not nvl(a.dichvu, 'a') in ('Mega+Fiber', 'MyTV')
--									and not nvl(a.dichvu, 'a') in ('VNP tra sau')
									and a.NHOM_DICHVU in ('Dichvu_Con lai')
			;
			---KIEM tra bang sau khi chay
					select sum(dthu_kpi) from
						(
						with pgd as ( select distinct MA_PB, MA_TO, MA_NV, THANG, dichvu, nhom_dichvu
												from ttkd_bsc.blkpi_dm_to_pgd pgd 
												where thang = 202407 and nhom_dichvu is not null
															)
									select a.*, pgd.ma_nv
													, case when exists (select 1 from pgd where ma_pb = a.ma_pb) then 1 else 0 end exit_ma_pb
													, case when exists (select 1 from pgd where ma_to = a.ma_to) then 1 else 0 end exit_ma_to
									from ttkd_bsc.temp_021_ldp1 a
												left join pgd on a.ma_to = pgd.ma_to and a.dichvu = pgd.dichvu and a.nhom_dichvu = pgd.nhom_dichvu and pgd.thang=202407
						) where exit_ma_pb + exit_ma_to = 2 and ma_nv is not null
		;

commit;
		
		update ttkd_bsc.bangluong_kpi_202407 a set hcm_dt_ptmoi_021='' where hcm_dt_ptmoi_021=0; 
		update ttkd_bsc.bangluong_kpi_202407 a set hcm_dt_ptmoi_021_vnptt='' where hcm_dt_ptmoi_021_vnptt=0;
		update ttkd_bsc.bangluong_kpi_202407 a set hcm_dt_ptmoi_021_vnpts='' where hcm_dt_ptmoi_021_vnpts=0; 
		update ttkd_bsc.bangluong_kpi_202407 a set hcm_dt_ptmoi_021_cdbr_mytv='' where hcm_dt_ptmoi_021_cdbr_mytv=0;
		update ttkd_bsc.bangluong_kpi_202407 a set hcm_dt_ptmoi_021_cntt='' where hcm_dt_ptmoi_021_cntt=0;
		*/ ----tam thoi khoa Thang 7	
		commit;
		rollback;
		
		select * from ttkd_bsc.blkpi_danhmuc;
		select * from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = 202407 ;
		select ma_nv, ten_vtcv, hcm_dt_ptmoi_021_vnptt, hcm_dt_ptmoi_021_vnpts, hcm_dt_ptmoi_021_cdbr_mytv, hcm_dt_ptmoi_021_cntt, HCM_DT_PTMOI_021 from ttkd_bsc.bangluong_kpi_202407 a;
		select * from ttkd_bsc.bangluong_kpi a where ma_kpi in ('HCM_DT_PTMOI_021') and thang = 202407;
		select * from ttkdhcm_ktnv.ID430_TTDANGKY_CHOTTHANG where thang = 202407 and manv_hrm = 'VNP016902';
--		update ttkd_bsc.dinhmuc_giao_dthu_ptm a
--					set KHDK = (select TONG_DTGIAO from ttkdhcm_ktnv.ID430_TTDANGKY_CHOTTHANG where thang = 202407 and MANV_HRM = a.ma_nv)
--		where thang = 202407-- and ma_nv = 'CTV021946'
--						and exists (select TONG_DTGIAO from ttkdhcm_ktnv.ID430_TTDANGKY_CHOTTHANG where thang = 202407 and MANV_HRM = a.ma_nv)
				;
--		select * from ttkdhcm_ktnv.ID430_TTDANGKY_CHOTTHANG where thang = 202407 and MANV_HRM not in (select ma_nv from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = 202407)
		;
--		update ttkd_bsc.dinhmuc_giao_dthu_ptm 
--					set NHOMVINATT_KQTH = 1000000 * NHOMVINATT_KQTH
--				where thang = 202407
--				;
		update ttkd_bsc.dinhmuc_giao_dthu_ptm 
					set KQTH = nvl(NHOMBRCD_KQTH, 0) + nvl(NHOMVINATS_KQTH, 0) + nvl(NHOMVINATT_KQTH, 0) + nvl(NHOMCNTT_KQTH, 0) + nvl(NHOMCONLAI_KQTH, 0)
				where thang = 202407
				;
		update ttkd_bsc.bangluong_kpi a set THUCHIEN = (select nvl(KQTH, 0)/1000000 from ttkd_bsc.dinhmuc_giao_dthu_ptm where thang = a.thang and ma_nv = a.ma_nv)
--				select * from  ttkd_bsc.bangluong_kpi a
				where ma_kpi in ('HCM_DT_PTMOI_021') and a.thang = 202407
				;
		----update DTHU thau 2 NV Phong GP
		update ttkd_bsc.bangluong_kpi
			set THUCHIEN = THUCHIEN + 19139500/1000000
--		select * from  ttkd_bsc.bangluong_kpi
		where thang = 202407 and ma_kpi = 'HCM_DT_PTMOI_021' and ma_nv in ('VNP027259', 'VNP017190')
		;
		update ttkd_bsc.bangluong_kpi a set GIAO = case when ma_vtcv in ('VNP-HNHCM_GP_3') then 16		---fix so theo Chu Minh KHKT
																							when ma_vtcv in ('VNP-HNHCM_KDOL_4') then 14.5		---fix so theo vb ap dung T6
																							when ma_vtcv in ('VNP-HNHCM_KDOL_5') then 107		---fix so theo vb ap dung T6
																							when ma_vtcv in ('VNP-HNHCM_KDOL_17') and ma_nv in ('VNP016759', 'VNP017436', 'VNP017854', 'VNP017144', 'VNP016547', 'VNP017443'
																														,'VNP016578', 'VNP017344', 'VNP017778', 'VNP017163', 'VNP016808', 'VNP016558', 'VNP016588','VNP017548')	
																										then 2.6
																							when ma_vtcv in ('VNP-HNHCM_KDOL_17') then 6.5		---fix so theo vb ap dung T6, nhung chua bik vi tri nào 2.6tr																							
																								else	(select round(TONG_DTGIAO/1000000, 3) from ttkd_bsc.dinhmuc_giao_dthu_ptm 
																											where thang = a.thang and ma_nv = a.ma_nv 
																														--	and trunc(dateinput) = '28/07/2024'
																										)
																					end
																		, tytrong = case when ma_vtcv in ('VNP-HNHCM_GP_3') then 40	---fix so theo vb ap dung T6 NV PS PGP
																									when ma_vtcv in ('VNP-HNHCM_KDOL_4', 'VNP-HNHCM_KDOL_5') then 85		---fix so theo vb ap dung T6 NV KDOL, TT KDOL
																									when ma_vtcv in ('VNP-HNHCM_KDOL_17') then 50		---fix so theo vb ap dung T6	NV OB BH
																									when ma_nv in ('VNP016799', 'VNP017843') and ma_vtcv = 'VNP-HNHCM_BHKV_6' then 100	--thay doi theo thang TSN KDDB
																									when ma_vtcv in ('VNP-HNHCM_BHKV_6', 'VNP-HNHCM_BHKV_42', 'VNP-HNHCM_BHKV_2') then 80	---fix so theo vb ap dung T7 KDDB, TT KDDB, PGD KV
																									when ma_vtcv in ('VNP-HNHCM_BHKV_51', 'VNP-HNHCM_BHKV_41') then 100	--thay doi theo thang TT CNTT KV, AM BHKV
																									when ma_vtcv in ('VNP-HNHCM_BHKV_28', 'VNP-HNHCM_BHKV_27', 'VNP-HNHCM_BHKV_22') then 60	--thay doi theo thang CHT, CHT kGDV, GDV
																						 end
																		, NGAYCONG = 23
				where a.ma_kpi in ('HCM_DT_PTMOI_021') and a.thang = 202407 
			;
		update ttkd_bsc.bangluong_kpi a set TYLE_THUCHIEN = case when GIAO = 0 then null
																												when ma_vtcv in ('VNP-HNHCM_BHKV_6', 'VNP-HNHCM_BHKV_41') ----KDDB, AM BHKV
																																		and giao < 13 and thuchien < 13 and ROUND(THUCHIEN/GIAO*100, 2) > 100
																															then 100
																												when ma_vtcv in ('VNP-HNHCM_BHKV_6', 'VNP-HNHCM_BHKV_41') ----KDDB, AM BHKV
																																		and giao < 13 and thuchien >= 13 
																															then ROUND(THUCHIEN/13 *100, 2)																															
																												when ma_vtcv in ('VNP-HNHCM_BHKV_22') ----GDV
																																		and giao < 8 and thuchien < 8 and ROUND(THUCHIEN/GIAO*100, 2) > 100
																															then 100
																												when ma_vtcv in ('VNP-HNHCM_BHKV_22') ----GDV
																																		and giao < 8 and thuchien >= 8 
																															then ROUND(THUCHIEN/8 *100, 2)
																												
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
				where ma_kpi in ('HCM_DT_PTMOI_021') and thang = 202407 
			;
		update ttkd_bsc.bangluong_kpi a set MUCDO_HOANTHANH = case 
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
																																						else round(100 + (1.2 * (TYLE_THUCHIEN - 100)), 2) end ---100% + 1.2 x (TLTH – 100%) -- max 150%
																												end
																													
				where ma_kpi in ('HCM_DT_PTMOI_021') and a.thang = 202407  --and ma_nv = 'VNP016902'
			;
		update ttkd_bsc.bangluong_kpi_202407 a set HCM_DT_PTMOI_021 = (select MUCDO_HOANTHANH from ttkd_bsc.bangluong_kpi 
																																		where ma_kpi in ('HCM_DT_PTMOI_021') and thang = 202407 and a.ma_nv = ma_nv) 
--		where ma_nv in ('VNP027259', 'VNP017190')
--		where ma_nv = 'CTV028802'
		;
commit;
rollback;
		
		create table ttkd_bsc.bangluong_kpi_202407_l3 as select * from ttkd_bsc.bangluong_kpi_202407;
		select * from  ttkd_bsc.bangluong_kpi where thang = 202407 and ma_kpi = 'HCM_DT_PTMOI_021' and ma_nv = 'CTV028802';
		select sum(HCM_DT_PTMOI_021) from  ttkd_bsc.bangluong_kpi_202407 where HCM_DT_PTMOI_021 is not null and ma_nv = 'CTV028802';
		;
		select a.ma_nv, a.ten_nv, a.ten_vtcv, a.ten_donvi, a.HCM_DT_PTMOI_021 HCM_DT_PTMOI_021_newa, b.HCM_DT_PTMOI_021 old
		from ttkd_bsc.bangluong_kpi_202407 a
				join ttkd_bsc.bangluong_kpi_202407_l3 b on a.ma_nv = b.ma_nv
		where a.HCM_DT_PTMOI_021 != b.HCM_DT_PTMOI_021
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
					where thang = 202407
;
	
	rollback;
	commit;
------------------------------------------------------------------------------------------------

select *
		from admin.v_hs_thuebao a
					join admin.v_file_hs b on a.FILE_ID = b.FILE_ID
					;

		
		-- san luong phát trien moi brcd, vnp tra sau
			---kiem tra co giao hay khong
		select distinct a.*, b.*, b.ten_vtcv from blkpi_danhmuc_kpi a, blkpi_danhmuc_kpi_vtcv b, nhanvien_202402 c  
				where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv       
							and a.thang_kt is null and b.thang_kt is null 
							and lower(a.ma_kpi)='hcm_sl_brvnp_001' 
			;




