/*
create table bangluong_kpi_202405_l6 as select * from bangluong_kpi_202405;  


ktra sau khi add du lieu thang do Xuan Vinh cung cap
select * from ttkd_bsc.dinhmuc_dthu_ptm where  thang=202405 ;
update ttkd_bsc.dinhmuc_dthu_ptm set  thang=202405 where thang is null;
update ttkd_bsc.dinhmuc_dthu_ptm set dt_giao_bsc='' where thang=202405 and dt_giao_bsc=0;
*/


select distinct a.*, b.*, c.ten_vtcv from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien_202405 c  
        where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv       
                    and a.thang_kt is null and b.thang_kt is null 
                    and a.ma_kpi='HCM_DT_PTMOI_021' ;
    
                    

drop table ttkd_bsc.bangluong_kpi_202405_ptm purge
	;
		create table ttkd_bsc.bangluong_kpi_202405_ptm as 
			 select ma_nv, ten_nv , ma_vtcv,ten_vtcv,ma_donvi, ten_donvi , ma_to ,ten_to, hcm_dt_ptmoi_021, hcm_dt_ptmoi_021_ts, hcm_dt_ptmoi_021_tt
			 from ttkd_bsc.bangluong_kpi_202405
			;


		alter table ttkd_bsc.bangluong_kpi_202405_ptm 
			add (giao number, tong number, hcm_dt_ptmoi_021_ldp number)
			;
          
          
		drop table ttkd_bsc.temp_trasau_canhan purge;
		desc ttkd_bsc.ghtt_vnpts;
		;
		select * from ttkd_bsc.ghtt_vnpts where ma_nv = 'VNP001752';
		----Tat ca dich vu, ngoai tru VNPtt
		create table ttkd_bsc.temp_trasau_canhan as
--		insert into ttkd_bsc.temp_trasau_canhan --(MANV_PTM, DTHU_KPI)
				select cast(manv_ptm as varchar(20)) ma_nv,  round(sum(dthu_kpi)/1000000,3) dthu_kpi 
				from (
						---dich vu ngoai ctr OR dvu khac VNPtt
						select thang_ptm, ma_gd, ma_tb, dich_vu, manv_ptm, doanhthu_kpi_nvptm dthu_kpi 
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi=202405 and (loaitb_id<>21 or ma_kh='GTGT rieng')
					union all
						select thang_ptm, ma_gd, ma_tb, dich_vu, manv_ptm, doanhthu_kpi_dnhm 
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi_dnhm=202405 and (loaitb_id<>21 or ma_kh='GTGT rieng')
					union all
					----Dthu nvho tro ban kenh ngoai
						select thang_ptm, ma_gd, ma_tb, dich_vu, manv_hotro, doanhthu_kpi_nvhotro 
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi_hotro = 202405
										and tyle_am is null and tyle_hotro is null and (loaitb_id<>21 or ma_kh='GTGT rieng')
					union all
						select cast(thang_ptm as number) thang_ptm, ma_gd, ma_tb, dich_vu, manv_hotro, doanhthu_kpi_nvhotro
						from ttkd_bsc.ct_bsc_ptm_pgp 
						where thang_tlkpi_hotro=202405 and (loaitb_id<>21 or ma_kh='GTGT rieng')
					union all
						select thang_ptm, ma_gd, ma_tb, dich_vu, manv_tt_dai, doanhthu_kpi_nvdai 
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi=202405 and (loaitb_id<>21 or ma_kh='GTGT rieng')
					union all
						----Gia han VNPts
						select thang, ma_kh, ma_tb, 'VNPTS' dich_vu, ma_nv, doanhthu_dongia 
						from ttkd_bsc.ghtt_vnpts 		--- Nguyen quan ly, chua co vb, toan noi mieng
						where thang=202405 and thang_giao is not null and ma_nv is not null
				)
--				where MANV_PTM in ('VNP017190','VNP027259')
				group by manv_ptm
		;
	create index ttkd_bsc.temp_trasau_canhan_manv on ttkd_bsc.temp_trasau_canhan (ma_nv)
	;

-- ca nhan
		--update bangluong_kpi_202405_ptm a set hcm_dt_ptmoi_021_ts='', hcm_dt_ptmoi_021_tt=''
		; 
		update ttkd_bsc.bangluong_kpi_202405_ptm a
		   set hcm_dt_ptmoi_021_ts=(select dthu_kpi from ttkd_bsc.temp_trasau_canhan  where ma_nv=a.ma_nv)
				-- ,hcm_dt_ptmoi_021_tt=(select dthu_kpi from temp_ptmtt_canhan a where manv_ptm=a.ma_nv) 
				
			;
		commit;							 

--		update ttkd_bsc.bangluong_kpi_202405_ptm a set tong='', hcm_dt_ptmoi_021=''
--		;
--		update ttkd_bsc.bangluong_kpi_202405_ptm a
--				set tong = nvl(hcm_dt_ptmoi_021_ts,0) + nvl(hcm_dt_ptmoi_021_tt,0)
--						, hcm_dt_ptmoi_021=nvl(hcm_dt_ptmoi_021_ts,0) + nvl(hcm_dt_ptmoi_021_tt,0)
--			-- select ma_nv, ma_vtcv, ten_vtcv, ten_donvi, hcm_dt_ptmoi_021_ts, hcm_dt_ptmoi_021_tt, tong, hcm_dt_ptmoi_021 from ttkd_bsc.bangluong_kpi_202405_ptm a
--			where (hcm_dt_ptmoi_021_ts is not null or hcm_dt_ptmoi_021_tt is not null)
----			and  ma_nv in ('VNP017190','VNP027259')
--		; 
 

-- to truong: thieu 0021_ts
	drop table ttkd_bsc.temp_totruong purge;
	;
	create table ttkd_bsc.temp_totruong as
			select ma_to, round(sum(doanhthu_kpi_to)/1000000,3) dthu_kpi
			from(
					select ma_to, doanhthu_kpi_to
					  from ttkd_bsc.ct_bsc_ptm a
					  where thang_tlkpi_to=202405 and (loaitb_id<>21 or loaitb_id is null)
				union all
					select ma_to, doanhthu_kpi_dnhm
					  from ttkd_bsc.ct_bsc_ptm a
					 where thang_tlkpi_dnhm_to=202405 and (loaitb_id<>21 or loaitb_id is null)
--				union all
--					select ma_to, doanhthu_kpi_nvhotro
--					  from ttkd_bsc.ct_bsc_ptm a
--					 where thang_tlkpi_hotro=202405 and (loaitb_id<>21 or loaitb_id is null)
				union all
					select ma_to, doanhthu_dongia 
					from ttkd_bsc.ghtt_vnpts		----a Nguyen quan ly
					  where thang=202405 and thang_giao is not null and ma_to is not null 
					  
						)group by ma_to
		;


  
		update ttkd_bsc.bangluong_kpi_202405_ptm a
		   set hcm_dt_ptmoi_021_ts=''
		 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi in ('HCM_DT_PTMOI_021') 
												and thang_kt is null and to_truong_pho is not null and ma_vtcv=a.ma_vtcv)
								;
						 
						 
		update ttkd_bsc.bangluong_kpi_202405_ptm a
		   set hcm_dt_ptmoi_021_ts=(select dthu_kpi from ttkd_bsc.temp_totruong where ma_to=a.ma_to)
		 -- select ma_nv, ma_to, ten_donvi, ten_vtcv, hcm_dt_ptmoi_021_ts from bangluong_kpi_202405_ptm a
		 where exists (select dthu_kpi from ttkd_bsc.temp_totruong where ma_to=a.ma_to) 
			and exists(select 1 from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi='HCM_DT_PTMOI_021' and thang_kt is null and to_truong_pho is not null and ma_vtcv=a.ma_vtcv);    
		;
		
--		update ttkd_bsc.bangluong_kpi_202405_ptm a
--				set tong = nvl(hcm_dt_ptmoi_021_ts,0) + nvl(hcm_dt_ptmoi_021_tt,0)
--					   ,hcm_dt_ptmoi_021 = nvl(hcm_dt_ptmoi_021_ts,0) + nvl(hcm_dt_ptmoi_021_tt,0)
--			-- select ma_nv, ma_vtcv, ten_vtcv, ten_donvi, hcm_dt_ptmoi_021_ts, hcm_dt_ptmoi_021_tt, tong, hcm_dt_ptmoi_021 from ttkd_bsc.bangluong_kpi_202405_ptm a
--			where (hcm_dt_ptmoi_021_ts is not null or hcm_dt_ptmoi_021_tt is not null)
		; 
commit;
---- ldp phu trach: tinh theo MA_TO va NHOM dich vu
		 select * from ttkd_bsc.temp_021_ldp;
	 create table ttkd_bsc.temp_021_ldp1 as
	select a.ma_pb, a.ma_to, b.dv_cap1, b.dv_cap2, round(sum(doanhthu_kpi_to)/1000000,3) dthu_kpi
									from (select ma_pb, ma_to, loaitb_id, doanhthu_kpi_to
												from ttkd_bsc.ct_bsc_ptm a
												where thang_tlkpi_to=202405 and (loaitb_id<>21 or loaitb_id is null)
												union all
															select ma_pb, ma_to, loaitb_id, doanhthu_kpi_dnhm
															from ttkd_bsc.ct_bsc_ptm a
															where thang_tlkpi_dnhm_to=202405 and (loaitb_id<>21 or loaitb_id is null)
												union all
															select ma_pb, ma_to, 20, doanhthu_dongia 
															from ttkd_bsc.ghtt_vnpts
															where thang=202405 and thang_giao is not null and ma_to is not null 
												) a
											left join ttkd_bsc.dm_loaihinh_hsqd b on a.loaitb_id = b.loaitb_id
											group by a.ma_pb, a.ma_to, b.dv_cap1, b.dv_cap2
			;
			drop table  ttkd_bsc.temp_021_ldp purge;
	create table ttkd_bsc.temp_021_ldp as	
			select a.*, pgd.ma_nv 
			from ttkd_bsc.temp_021_ldp1 a
						join ttkd_bsc.blkpi_dm_to_pgd pgd
							on a.ma_to = pgd.ma_to and pgd.thang=202405 
										and lower(pgd.ma_kpi) in ('hcm_dt_ptmoi_021')
			where a.ma_pb in ('VNP0702300', 'VNP0702400', 'VNP0702500')
			union all
						select a.*, pgd.ma_nv 
						from ttkd_bsc.temp_021_ldp1 a
									 join ttkd_bsc.blkpi_dm_to_pgd pgd
										on a.ma_to = pgd.ma_to and (a.dv_cap1 = pgd.dv_cap1 or a.dv_cap2 = pgd.dv_cap2) and pgd.thang=202405 
													and lower(pgd.ma_kpi) in ('hcm_dt_ptmoi_021')
						where a.ma_pb not in ('VNP0702300', 'VNP0702400', 'VNP0702500')
			;

                                                                         
   
		update ttkd_bsc.bangluong_kpi_202405_ptm a
		   set hcm_dt_ptmoi_021_ts =''
		 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi='HCM_DT_PTMOI_021' and thang_kt is null and giamdoc_phogiamdoc is not null and ma_vtcv=a.ma_vtcv)
					and exists(select 1 from ttkd_bsc.temp_021_ldp where ma_nv=a.ma_nv)
					;

            
		update ttkd_bsc.bangluong_kpi_202405_ptm a
		   set hcm_dt_ptmoi_021_ts=(select sum(dthu_kpi) from ttkd_bsc.temp_021_ldp where ma_nv=a.ma_nv)
		where  exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi='HCM_DT_PTMOI_021' and thang_kt is null and giamdoc_phogiamdoc is not null and ma_vtcv=a.ma_vtcv)
					and exists(select * from ttkd_bsc.temp_021_ldp where ma_nv=a.ma_nv)
					;      

		select * from ttkd_bsc.bangluong_kpi_202405_ptm;
-- -_da xong

commit;
	


-- update bangluong_kpi_202405:    
		create table ttkd_bsc.bangluong_kpi_202405_l2 as select * from ttkd_bsc.bangluong_kpi_202405 a;
		update ttkd_bsc.bangluong_kpi_202405 a 
				set HCM_DT_PTMOI_021_TS = '' 
			where  hcm_dt_ptmoi_021 is not null 
			;    
			
		update ttkd_bsc.bangluong_kpi_202405 a 
				set HCM_DT_PTMOI_021_TS = (select HCM_DT_PTMOI_021_TS 
																											from ttkd_bsc.bangluong_kpi_202405_ptm where ma_nv=a.ma_nv) 
			where exists(select 1 from ttkd_bsc.bangluong_kpi_202405_ptm where HCM_DT_PTMOI_021_TS is not null and ma_nv=a.ma_nv) 
			;
		update ttkd_bsc.bangluong_kpi_202405 a set HCM_DT_PTMOI_021= nvl(HCM_DT_PTMOI_021_TS, 0) + nvl(HCM_DT_PTMOI_021_TT, 0); 		
		
		update ttkd_bsc.bangluong_kpi_202405 a set hcm_dt_ptmoi_021='' where hcm_dt_ptmoi_021=0; 
		update ttkd_bsc.bangluong_kpi_202405 a set hcm_dt_ptmoi_021_ts='' where hcm_dt_ptmoi_021_ts=0;
			
		commit;    
		select ma_nv, HCM_DT_PTMOI_021_TS, HCM_DT_PTMOI_021_TT, HCM_DT_PTMOI_021 from ttkd_bsc.bangluong_kpi_202405 a;
    
/*
select distinct a.*, c.ten_vtcv
                ,(select count(*) from bangluong_kpi_202405 where ma_vtcv=b.ma_vtcv ) sl_nv
                ,(select count(*) from bangluong_kpi_202405 where hcm_dt_ptmoi_021 is not null and ma_vtcv=b.ma_vtcv ) slnv_dadanhgia
        from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien_202405 c  
        where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv 
                   and a.thang_kt is null and b.thang_kt is null 
                    and lower(a.ma_kpi)='hcm_dt_ptmoi_021' ;
 
                    
select a.ten_donvi, a.ma_to, a.ten_to, a.ma_nv_hrm,  a.ten_nv, a.ma_vtcv, a.ten_vtcv
             ,a.hcm_dt_ptmoi_021 hcm_dt_ptmoi_021_new, b.hcm_dt_ptmoi_021 hcm_dt_ptmoi_021_old,
             nvl(a.hcm_dt_ptmoi_021,0) - nvl(b.hcm_dt_ptmoi_021,0) chechlech
  from bangluong_kpi_202405 a, bangluong_kpi_202405_l5 b 
where a.ma_nv=b.ma_nv 
            and nvl(a.hcm_dt_ptmoi_021,0)<>nvl(b.hcm_dt_ptmoi_021,0)   
order by (a.hcm_dt_ptmoi_021 - b.hcm_dt_ptmoi_021);
                    

*/

------------------------------------------------------------------------------------------------

-- HCM_DT_PTMOI_052 Doanh thu dich vu di dong ptm trong tháng (VNPtt + VNPts) - NEW 03/2023: xet 4 thang nhu _021
select distinct a.*, b.*, b.ten_vtcv from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien_202405 c  
        where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv       
                    and a.thang_kt is null and b.thang_kt is null 
                    and a.ma_kpi='HCM_DT_PTMOI_052' ;


	drop table ttkd_bsc.temp_vnpts purge;
	;
	create table ttkd_bsc.temp_vnpts as
			select manv_ptm, round(sum(dthu_kpi)/1000000,3) dthu_kpi 
			from (
					select thang_ptm, ma_gd, ma_tb, dich_vu, manv_ptm,doanhthu_kpi_nvptm dthu_kpi 
					from ttkd_bsc.ct_bsc_ptm 
					where thang_tlkpi=202405 and loaitb_id=20
				union all
					select thang_ptm, ma_gd, ma_tb, dich_vu, manv_ptm,doanhthu_kpi_dnhm 
					from ttkd_bsc.ct_bsc_ptm 
					where thang_tlkpi_dnhm=202405 and loaitb_id=20
				union all
				
				----ca nhan ban DIGI, WEB
					select thang_ptm, ma_gd, ma_tb, dich_vu, manv_hotro,doanhthu_kpi_nvhotro 
					from ttkd_bsc.ct_bsc_ptm 
					where thang_tlkpi=202405 and loaitb_id=20
								and tyle_am is null and tyle_hotro is null
				union all
					select thang_ptm, ma_gd, ma_tb, dich_vu, manv_tt_dai,doanhthu_kpi_nvdai 
					from ttkd_bsc.ct_bsc_ptm 
					where thang_tlkpi=202405 and loaitb_id=20
				union all
					select thang, ma_kh, ma_tb, 'VNPTS', ma_nv, doanhthu_dongia 
					from ttkd_bsc.ghtt_vnpts
						  where thang=202405 and thang_giao is not null and ma_nv is not null 
			)group by manv_ptm
			;
	create index temp_vnpts_manv on temp_vnpts (manv_ptm)
	;
	
	alter table ttkd_bsc.bangluong_kpi_202405_ptm add didong_trasau number
	;
	update ttkd_bsc.bangluong_kpi_202405_ptm a set hcm_dt_ptmoi_052='', didong_trasau=''
	;
   
	update ttkd_bsc.bangluong_kpi_202405_ptm a
	   set didong_trasau=(select dthu_kpi from ttkd_bsc.temp_vnpts where manv_ptm=a.ma_nv)
	   ;

-- ca nhan:	
	 update ttkd_bsc.bangluong_kpi_202405_ptm a
		  set hcm_dt_ptmoi_052 = didong_trasau 
		where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
							where ma_kpi in ('HCM_DT_PTMOI_052')  
									and thang_kt is null and to_truong_pho is null and giamdoc_phogiamdoc is null and ma_vtcv=a.ma_vtcv)
	;
                commit;
                
-- kpi to truong: 
	drop table ttkd_bsc.temp_didong_totruong purge;
	create table ttkd_bsc.temp_didong_totruong as
				select ma_to, sum(didong_trasau) dthu_kpi--, sum(dthu_kpi_bs) dthu_kpi_bs 
				from ttkd_bsc.bangluong_kpi_202405_ptm a
				group by ma_to;

                            
		update ttkd_bsc.bangluong_kpi_202405_ptm a
			set hcm_dt_ptmoi_052=(select nvl(dthu_kpi,0) from ttkd_bsc.temp_didong_totruong where ma_to=a.ma_to)
		 -- select ma_nv, ma_to, ma_vtcv, ten_vtcv, hcm_dt_ptmoi_052, hcm_dt_ptm_052_ttg from bangluong_kpi_202405_ptm a
		 where exists (select 1 from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi in ('HCM_DT_PTMOI_052') and thang_kt is null and to_truong_pho is not null and ma_vtcv=a.ma_vtcv)
					-- and exists (select 1 from temp_didong_totruong where ma_to=a.ma_to)
					;
commit;
                                    
---- ldp phu trach:
		drop table ttkd_bsc.temp_052_ldp purge;
		create table ttkd_bsc.temp_052_ldp as  
		select pgd.ma_pb, pgd.ma_nv, bl.ten_donvi, (select ten_nv from ttkd_bsc.nhanvien_202405 where ma_nv=pgd.ma_nv) ten_nv, 
							round(sum(bl.didong_trasau) ,3) dthu_kpi--, sum(bl.dthu_kpi_bs) dthu_kpi_bs
		   from ttkd_bsc.bangluong_kpi_202405_ptm bl,  ttkd_bsc.blkpi_dm_to_pgd pgd
			where bl.ma_to = pgd.ma_to and pgd.thang=202405 and pgd.ma_kpi in ('HCM_DT_PTMOI_052')
						--and not exists (select 1 from nv_thuviec where thang=202405 and ma_to=pgd.ma_to and ma_nv=bl.ma_nv)
			group by pgd.ma_pb, pgd.ma_nv, bl.ten_donvi
			;
			
		update ttkd_bsc.bangluong_kpi_202405_ptm a
		   set hcm_dt_ptmoi_052 = ''
		 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi in ('HCM_DT_PTMOI_052') 
											and thang_kt is null and giamdoc_phogiamdoc is not null and ma_vtcv=a.ma_vtcv)
					and exists(select 1 from ttkd_bsc.temp_052_ldp where ma_nv=a.ma_nv);
					
					
		update ttkd_bsc.bangluong_kpi_202405_ptm a
		   set hcm_dt_ptmoi_052 = (select nvl(dthu_kpi,0) from ttkd_bsc.temp_052_ldp where ma_nv=a.ma_nv)
		-- select * from bangluong_kpi_202405_ptm a
		where  exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi in ('HCM_DT_PTMOI_052') and thang_kt is null and giamdoc_phogiamdoc is not null and ma_vtcv=a.ma_vtcv)
					and exists (select 1 from ttkd_bsc.blkpi_dm_to_pgd pgd
										 where pgd.thang=202405 and pgd.ma_kpi in ('HCM_DT_PTMOI_052') and ma_nv=a.ma_nv)
					and exists(select * from ttkd_bsc.temp_052_ldp where ma_nv=a.ma_nv)
				;      
		 
		 
		 commit;
 
-- update bangluong_kpi_202405 
		update ttkd_bsc.bangluong_kpi_202405 a set HCM_DT_PTMOI_052='';     
		update ttkd_bsc.bangluong_kpi_202405 a set HCM_DT_PTMOI_052 = 
				(select HCM_DT_PTMOI_052 from ttkd_bsc.bangluong_kpi_202405_ptm where hcm_dt_ptmoi_052>0 and ma_nv=a.ma_nv) 
			-- select hcm_dt_ptmoi_052, hcm_dt_ptmoi_021 from bangluong_kpi_202405 a
			where exists(select hcm_dt_ptmoi_052 from ttkd_bsc.bangluong_kpi_202405_ptm where hcm_dt_ptmoi_052>0 and ma_nv=a.ma_nv) 
			;
			
		update ttkd_bsc.bangluong_kpi_202405 a set HCM_DT_PTMOI_052 = nvl(HCM_DT_PTMOI_052, 0) + nvl(HCM_DT_PTMOI_021_TT, 0)
		;
				
		commit;  
		---da xong
		
		select ma_nv, HCM_DT_PTMOI_052, HCM_DT_PTMOI_021_TT, HCM_DT_PTMOI_021_TS, HCM_DT_PTMOI_021  from ttkd_bsc.bangluong_kpi_202405_ptm where ma_nv = 'VNP017601';
		select ma_nv, HCM_DT_PTMOI_052, HCM_DT_PTMOI_021_TT, HCM_DT_PTMOI_021_TS, HCM_DT_PTMOI_021  from ttkd_bsc.bangluong_kpi_202405 where ma_nv = 'VNP017601';

---kiem tra HCM_DT_PTMOI_052
			select distinct a.*, b.*, c.ten_vtcv 
							,(select count(*) from ttkd_bsc.bangluong_kpi_202405 where ma_vtcv=b.ma_vtcv ) slnv
							,(select count(*) from ttkd_bsc.bangluong_kpi_202405 where hcm_dt_ptmoi_052 is not null and ma_vtcv=b.ma_vtcv ) slnv_dadanhgia
					from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien_202405 c  
					where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv 
							   and a.thang_kt is null and b.thang_kt is null 
								and lower(a.ma_kpi)='hcm_dt_ptmoi_052' ;
			  
								
			select a.ten_donvi, a.ten_to, a.ma_nv,  a.ten_nv, a.ma_vtcv, a.ten_vtcv
						 ,a.hcm_dt_ptmoi_052 hcm_dt_ptmoi_052_new, b.hcm_dt_ptmoi_052 hcm_dt_ptmoi_052_old,
						 nvl(a.hcm_dt_ptmoi_052,0) - nvl(b.hcm_dt_ptmoi_052,0) chechlech
			  from ttkd_bsc.bangluong_kpi_202405 a, ttkd_bsc.bangluong_kpi_202405_l3 b 
			where a.ma_nv=b.ma_nv 
						and nvl(a.hcm_dt_ptmoi_052,0)<>nvl(b.hcm_dt_ptmoi_052,0)    
			order by (a.hcm_dt_ptmoi_052 - b.hcm_dt_ptmoi_052);
				

			select a.ten_donvi, a.ten_to, a.ma_nv,  a.ten_nv, a.ten_vtcv
						 ,a.hcm_dt_ptmoi_052 new, b.hcm_dt_ptmoi_052 old,
						 nvl(a.hcm_dt_ptmoi_052,0) - nvl(b.hcm_dt_ptmoi_052,0) chechlech
			  from bangluong_kpi_202405 a, bangluong_kpi_202405_l2 b 
			where a.ma_nv=b.ma_nv 
						and nvl(a.hcm_dt_ptmoi_052,0)<>nvl(b.hcm_dt_ptmoi_052,0)    
			order by (a.hcm_dt_ptmoi_052 - b.hcm_dt_ptmoi_052)
			;


--- DTHU CNTT:
/*-- Kiem tra danh muc loaitb_id thieu trong danh muc
insert into ttkd_bsc.dm_loaihinh_hsqd (DICHVUVT_ID, LOAITB_ID, LOAIHINH_TB)
    select distinct dichvuvt_id,loaitb_id, dich_vu --, (select loaihinh_tb from css_hcm.loaihinh_tb@ttkddb where loaitb_id=a.loaitb_id)loaihinh_tb
    from ttkd_bsc.ct_bsc_ptm a
    where thang_ptm=202405
        and not exists(select * from ttkd_bsc.dm_loaihinh_hsqd where loaitb_id=a.loaitb_id);
        
select distinct b.loaitb_id, b.loaihinh_tb, b.dv_cap2
  from ct_bsc_ptm a, ttkd_bsc.dm_loaihinh_hsqd b
    where b.loaitb_id<>21 and a.loaitb_id=b.loaitb_id and a.thang_tlkpi=202405 and b.dv_cap2 is null;

*/

	select HCM_DT_PTMOI_044 
	from ttkd_bsc.bangluong_kpi_202405 a
    where HCM_DT_PTMOI_044 is not null
			and exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
						where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and giamdoc_phogiamdoc is not null
						  and ma_vtcv=a.ma_vtcv
						 )
					  ;
                      
                      
--TT = tat ca NV trong to khong loai tru thu viec hay CTV
		select loaitb_id from ttkd_bsc.ct_bsc_ptm a
		where thang_tlkpi=202405 and dichvuvt_id in (13,14,15,16) 
				and exists(select * from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 is null and loaitb_id=a.loaitb_id)
				;


----------- Ca nhan 
		drop table ttkd_bsc.temp_cntt purge;
		;
		
--		select * from ttkd_bsc.temp_cntt;
		create table ttkd_bsc.temp_cntt as
				select manv_ptm ma_nv,sum(doanhthu_kpi_nvptm)dthu
				  from(
						select manv_ptm,doanhthu_kpi_nvptm
						  from ttkd_bsc.ct_bsc_ptm a
								where thang_tlkpi=202405
											  and (exists(select * from ttkd_bsc.dm_loaihinh_hsqd 
																		where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id=a.loaitb_id)
															or loaitb_id = 999
													)
					union all
						select manv_ptm,doanhthu_kpi_dnhm
						  from ttkd_bsc.ct_bsc_ptm a
						 where thang_tlkpi_dnhm=202405
						   and (exists(select * from ttkd_bsc.dm_loaihinh_hsqd 
																		where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id=a.loaitb_id)
															or loaitb_id=999
													)
				)group by manv_ptm
		;
 
-- Kiem tra:
		select distinct loaitb_id from ttkd_bsc.ct_bsc_ptm where thang_tlkpi=202405; and loaitb_id is null
		;


		update ttkd_bsc.bangluong_kpi_202405 a
		   set HCM_DT_PTMOI_044 = ''
		 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
						where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and to_truong_pho is null and giamdoc_phogiamdoc is null
						  and ma_vtcv=a.ma_vtcv)
			;
			  
		update ttkd_bsc.bangluong_kpi_202405 a
		   set HCM_DT_PTMOI_044=(select round(dthu/1000000,3) from ttkd_bsc.temp_cntt where ma_nv=a.ma_nv_hrm) 
		 where exists(select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
					   where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and to_truong_pho is null and giamdoc_phogiamdoc is null
						 and ma_vtcv=a.ma_vtcv)
			;


		select a.ma_nv, a.ten_vtcv, a.ten_donvi, HCM_DT_PTMOI_044 from ttkd_bsc.bangluong_kpi_202405 a
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
							select ma_to,doanhthu_kpi_to
							  from ttkd_bsc.ct_bsc_ptm a
							 where thang_tlkpi_to=202405
							   and (exists(select * from ttkd_bsc.dm_loaihinh_hsqd 
																		where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id=a.loaitb_id)
															or loaitb_id=999
													)
							 union all
							select ma_to, doanhthu_kpi_dnhm_phong
							  from ttkd_bsc.ct_bsc_ptm a
							 where thang_tlkpi_dnhm_to=202405
							   and (exists(select * from ttkd_bsc.dm_loaihinh_hsqd 
																		where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id=a.loaitb_id)
															or loaitb_id=999
													)
			)group by ma_to
			;
			 

			update ttkd_bsc.bangluong_kpi_202405 a
			   set HCM_DT_PTMOI_044=''
			 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
							where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and to_truong_pho is not null
							  and ma_vtcv=a.ma_vtcv)
							  ;
				  
			update ttkd_bsc.bangluong_kpi_202405 a
			   set HCM_DT_PTMOI_044=(select round(dthu/1000000,3) from ttkd_bsc.temp_cntt where ma_to=a.ma_to) 
			 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
							where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and to_truong_pho is not null
							  and ma_vtcv=a.ma_vtcv)
							  ;
							  commit;

			select a.ma_nv, a.ten_vtcv, a.ten_donvi, HCM_DT_PTMOI_044 
			from  ttkd_bsc.bangluong_kpi_202405 a
			 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
							where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and to_truong_pho is not null
							  and ma_vtcv=a.ma_vtcv);
							  
                                   
----------- PGD
		drop table ttkd_bsc.temp_cntt purge;
		create table ttkd_bsc.temp_cntt as
				select manv_pgd,dthu 
				  from (select ma_to,sum(doanhthu_kpi_phong)dthu
						  from (select ma_to,doanhthu_kpi_phong     
								  from ttkd_bsc.ct_bsc_ptm a
								 where thang_tlkpi_to=202405 -- thang_tlkpi_phong=202405
								   and (exists(select * from ttkd_bsc.dm_loaihinh_hsqd 
																		where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id=a.loaitb_id)
															or loaitb_id=999
													)
										
								 union all
								select ma_to,doanhthu_kpi_dnhm_phong
								  from ttkd_bsc.ct_bsc_ptm a
								 where thang_tlkpi_dnhm_to=202405 -- thang_tlkpi_dnhm_phong=202405
								  and (exists(select * from ttkd_bsc.dm_loaihinh_hsqd 
																		where dv_cap2 in ('Dich vu so doanh nghiep') and loaitb_id=a.loaitb_id)
															or loaitb_id=999
													)
							   )
						  group by ma_to)x
					  ,(select ma_nv manv_pgd,ma_to from ttkd_bsc.blkpi_dm_to_pgd where thang=202405 and upper(ma_kpi)='HCM_DT_PTMOI_044')y
				 where x.ma_to=y.ma_to
		;
		 
		 
		update ttkd_bsc.bangluong_kpi_202405 a
		   set HCM_DT_PTMOI_044=''
		 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
						where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and giamdoc_phogiamdoc is not null
						  and ma_vtcv=a.ma_vtcv);
			  
		update ttkd_bsc.bangluong_kpi_202405 a
		   set HCM_DT_PTMOI_044=(select round(sum(dthu)/1000000,3) from ttkd_bsc.temp_cntt where manv_pgd=a.ma_nv) 
		 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
						where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and giamdoc_phogiamdoc is not null
						  and ma_vtcv=a.ma_vtcv);
						  
		commit;                  

---END HCM_DT_PTMOI_044

		 select distinct a.*, b.ma_vtcv, c.ten_vtcv
						,(select count(*) from ttkd_bsc.bangluong_kpi_202405 
								where hcm_dt_ptmoi_044 is not null and ma_vtcv=b.ma_vtcv ) slnv_dadanhgia
				from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien_202405 c  
				where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv 
						   and a.thang_kt is null and b.thang_kt is null 
							and lower(a.ma_kpi)='hcm_dt_ptmoi_044' ;
		 
		 
		select a.ten_donvi, a.ten_to, a.ma_nv_hrm,  a.ten_nv, a.ma_vtcv, a.ten_vtcv
					 ,a.hcm_dt_ptmoi_044 new, b.hcm_dt_ptmoi_044 old,
					 nvl(a.hcm_dt_ptmoi_044,0) - nvl(b.hcm_dt_ptmoi_044,0) chechlech
		  from ttkd_bsc.bangluong_kpi_202405 a, ttkd_bsc.bangluong_kpi_202405_l7 b 
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


/*--------------------------------------------------------------------------------------

select ma_nv, hcm_dt_ptmoi_021_ts, hcm_dt_ptmoi_021_tt, hcm_dt_ptmoi_021 from ttkd_bsc.bangluong_kpi_202405;

select * from bangluong_kpi_202405;
select a.ten_donvi, a.ten_to, a.ma_nv, a.ten_nv, a.ten_vtcv,a.ma_nv_hrm, a.hcm_dt_ptmoi_021_ts , a.hcm_dt_ptmoi_021_tt , a.hcm_dt_ptmoi_021
from bangluong_kpi_202405 a
    where a.hcm_dt_ptmoi_021 is not null;

select sum(hcm_dt_ptmoi_021_ts)+sum(hcm_dt_ptmoi_021_tt)  from ttkd_bsc.bangluong_kpi_202405 
union all
select sum(hcm_dt_ptmoi_021) from ttkd_bsc.bangluong_kpi_202405 ;


-- gui PNS:
-- ptm:
select a.ten_donvi, a.ma_to, a.ten_to, a.ma_nv, a.ma_vtcv, a.ten_nv, a.ten_vtcv, a.ma_nv_hrm   ,        
             a.hcm_dt_ptmoi_021 hcm_dt_ptmoi_021_new, 
             b.hcm_dt_ptmoi_021 hcm_dt_ptmoi_021_old,
            nvl(a.hcm_dt_ptmoi_021,0)-nvl(b.hcm_dt_ptmoi_021,0) chechlech

    from bangluong_kpi_202405 a, bangluong_kpi_202405_l4 b 
    where a.ma_nv=b.ma_nv and nvl(a.hcm_dt_ptmoi_021,0)<>nvl(b.hcm_dt_ptmoi_021,0)

order by (a.hcm_dt_ptmoi_021 - b.hcm_dt_ptmoi_021);

-- hcm_dt_ptmoi_052
select a.ten_donvi, a.ma_to, a.ten_to, a.ma_nv, a.ma_vtcv, a.ten_nv, a.ten_vtcv, a.ma_nv_hrm   ,        
             a.hcm_dt_ptmoi_052 hcm_dt_ptmoi_052_new, 
             b.hcm_dt_ptmoi_052 hcm_dt_ptmoi_052_old,
            nvl(a.hcm_dt_ptmoi_052,0)-nvl(b.hcm_dt_ptmoi_052,0) chechlech

    from bangluong_kpi_202405 a, bangluong_kpi_202405_l21 b 
    where a.ma_nv=b.ma_nv and nvl(a.hcm_dt_ptmoi_052,0)<>nvl(b.hcm_dt_ptmoi_052,0)

order by (a.hcm_dt_ptmoi_052 - b.hcm_dt_ptmoi_052);


-- DT CNTT:
create table bangluong_kpi_202405_l7 as select * from bangluong_kpi_202405;
select a.ten_donvi, a.ten_to, a.ma_nv, a.ten_nv, a.ten_vtcv, a.ma_nv_hrm   
             ,a.hcm_dt_ptmoi_044 hcm_dt_ptmoi_044_new,
             b.hcm_dt_ptmoi_044 hcm_dt_ptmoi_044_old,
            nvl(a.hcm_dt_ptmoi_044,0)-nvl(b.hcm_dt_ptmoi_044,0) chechlech

    from bangluong_kpi_202405 a, bangluong_kpi_202405_l7 b 
    where a.ma_nv=b.ma_nv 
            and ( (a.hcm_dt_ptmoi_044<>b.hcm_dt_ptmoi_044)       
                        or (a.hcm_dt_ptmoi_044 is null and b.hcm_dt_ptmoi_044 is not null)
                        or (a.hcm_dt_ptmoi_044 is not null and b.hcm_dt_ptmoi_044 is null)
                      )  
order by (a.hcm_dt_ptmoi_044 - b.hcm_dt_ptmoi_044);


--hcm_tb_appbh_003
select a.ten_donvi, a.ten_to, a.ma_nv, a.ten_nv, a.ten_vtcv, a.ma_nv_hrm   
             ,a.hcm_tb_appbh_003 hcm_tb_appbh_003_new,
             b.hcm_tb_appbh_003 hcm_tb_appbh_003_old,
            nvl(a.hcm_tb_appbh_003,0)-nvl(b.hcm_tb_appbh_003,0) chechlech

    from bangluong_kpi_202405 a, bangluong_kpi_202405_l4 b 
    where a.ma_nv=b.ma_nv 
            and ( (a.hcm_tb_appbh_003<>b.hcm_tb_appbh_003)       
                        or (a.hcm_tb_appbh_003 is null and b.hcm_tb_appbh_003 is not null)
                        or (a.hcm_tb_appbh_003 is not null and b.hcm_tb_appbh_003 is null)
                      )  
order by (a.hcm_tb_appbh_003 - b.hcm_tb_appbh_003);


--hcm_ct_cluoc_001

select a.ten_donvi, a.ma_to, a.ten_to, a.ma_nv, a.ma_vtcv, a.ten_nv, a.ten_vtcv, a.ma_nv_hrm   ,        
             a.hcm_ct_cluoc_001 hcm_ct_cluoc_001_new, 
             b.hcm_ct_cluoc_001 hcm_ct_cluoc_001_old,
            nvl(a.hcm_ct_cluoc_001,0)-nvl(b.hcm_ct_cluoc_001,0) chechlech
    from bangluong_kpi_202405 a, bangluong_kpi_202405_l2 b 
    where a.ma_nv=b.ma_nv and nvl(a.hcm_ct_cluoc_001,0)<>nvl(b.hcm_ct_cluoc_001,0)
order by (a.hcm_ct_cluoc_001 - b.hcm_ct_cluoc_001);


-- C2:
select a.ten_donvi, a.ten_to, a.ma_nv, a.ten_nv, a.ten_vtcv, a.ma_nv_hrm   
             ,a.HCM_CT_CLUOC_001 HCM_CT_CLUOC_001_new,
             b.HCM_CT_CLUOC_001 HCM_CT_CLUOC_001_old,
            nvl(a.HCM_CT_CLUOC_001,0)-nvl(b.HCM_CT_CLUOC_001,0) chechlech

    from bangluong_kpi_202405 a, temp_dc b 
    where a.ma_nv=b.ma_nv 
            and ( (a.HCM_CT_CLUOC_001<>b.HCM_CT_CLUOC_001)       
                        or (a.HCM_CT_CLUOC_001 is null and b.HCM_CT_CLUOC_001 is not null)
                        or (a.HCM_CT_CLUOC_001 is not null and b.HCM_CT_CLUOC_001 is null)
                      )  
order by (a.HCM_CT_CLUOC_001 - b.HCM_CT_CLUOC_001);



select a.ten_donvi, a.ma_to, a.ten_to, a.ma_nv, a.ma_vtcv, a.ten_nv, a.ten_vtcv, a.ma_nv_hrm   ,        
             a.hcm_tb_giaha_023 hcm_tb_giaha_023_new, 
             b.hcm_tb_giaha_023 hcm_tb_giaha_023_old,
            nvl(a.hcm_tb_giaha_023,0)-nvl(b.hcm_tb_giaha_023,0) chechlech
    from bangluong_kpi_202405 a, bangluong_kpi_202405_l2 b 
    where a.ma_nv=b.ma_nv and nvl(a.hcm_tb_giaha_023,0)<>nvl(b.hcm_tb_giaha_023,0)
order by (a.hcm_tb_giaha_023 - b.hcm_tb_giaha_023);


select a.ten_donvi, a.ma_to, a.ten_to, a.ma_nv, a.ma_vtcv, a.ten_nv, a.ten_vtcv, a.ma_nv_hrm   ,        
             a.hcm_tb_giaha_022 hcm_tb_giaha_022_new, 
             b.hcm_tb_giaha_022 hcm_tb_giaha_022_old,
            nvl(a.hcm_tb_giaha_022,0)-nvl(b.hcm_tb_giaha_022,0) chechlech
    from bangluong_kpi_202405 a, bangluong_kpi_202405_l2 b 
    where a.ma_nv=b.ma_nv and nvl(a.hcm_tb_giaha_022,0)<>nvl(b.hcm_tb_giaha_022,0)
order by (a.hcm_tb_giaha_022 - b.hcm_tb_giaha_022);


select * from ttkd_bsc.bangluong_kpi_202405;
*/