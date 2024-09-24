/*
create table bangluong_kpi_202404_l6 as select * from bangluong_kpi_202404;  


ktra sau khi add du lieu thang do Xuan Vinh cung cap
select * from ttkd_bsc.dinhmuc_dthu_ptm where  thang=202404 ;
update ttkd_bsc.dinhmuc_dthu_ptm set  thang=202404 where thang is null;
update ttkd_bsc.dinhmuc_dthu_ptm set dt_giao_bsc='' where thang=202404 and dt_giao_bsc=0;
*/


select distinct a.*, b.*, c.ten_vtcv from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien_202404 c  
        where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv       
                    and a.thang_kt is null and b.thang_kt is null 
                    and a.ma_kpi='HCM_DT_PTMOI_021' ;
    
                    

drop table ttkd_bsc.bangluong_kpi_202404_ptm purge
	;
		create table ttkd_bsc.bangluong_kpi_202404_ptm as 
			 select ma_nv, ten_nv , ma_vtcv,ten_vtcv,ma_donvi, ten_donvi , ma_to ,ten_to, hcm_dt_ptmoi_021, hcm_dt_ptmoi_021_ts, hcm_dt_ptmoi_021_tt
			 from ttkd_bsc.bangluong_kpi_202404
			;


		alter table ttkd_bsc.bangluong_kpi_202404_ptm 
			add (giao number, tong number, hcm_dt_ptmoi_021_ldp number)
			;
                 

-- so giao: theo muc quy dinh: luu y dinh muc giao nay chi xet rieng cho D500new
			update ttkd_bsc.bangluong_kpi_202404_ptm a set giao=''
				;
			update ttkd_bsc.bangluong_kpi_202404_ptm a 
					set giao=(select round(dt_giao_bsc/1000000,3) from ttkd_bsc.dinhmuc_dthu_ptm
									  where thang=202404 and dt_giao_bsc>0 and ma_nv=a.ma_nv)
				where exists (select round(dt_giao_bsc/1000000,3) from ttkd_bsc.dinhmuc_dthu_ptm
										where thang=202404 and dt_giao_bsc>0 and ma_nv=a.ma_nv)
				;
          
          
          
		drop table ttkd_bsc.temp_trasau_canhan purge;
		desc ttkd_bsc.ghtt_vnpts;
		;
		----Tat ca dich vu, ngoai tru VNPtt
		create table ttkd_bsc.temp_trasau_canhan as
		
--		insert into ttkd_bsc.temp_trasau_canhan --(MANV_PTM, DTHU_KPI)
		
				select cast(manv_ptm as varchar(20)) ma_nv,  round(sum(dthu_kpi)/1000000,3) dthu_kpi 
				from (
						---dich vu ngoai ctr OR dvu khac VNPtt
						select thang_ptm, ma_gd, ma_tb, dich_vu, manv_ptm, doanhthu_kpi_nvptm dthu_kpi 
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng')
					union all
						select thang_ptm, ma_gd, ma_tb, dich_vu, manv_ptm, doanhthu_kpi_dnhm 
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi_dnhm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng')
					union all
						select cast(thang_ptm as number) thang_ptm, ma_gd, ma_tb, dich_vu, manv_hotro, doanhthu_kpi_nvhotro 
						from ttkd_bsc.ct_bsc_ptm_pgp 
						where thang_tlkpi_hotro=202404 --and (loaitb_id<>21 or ma_kh='GTGT rieng')
					union all
						select thang_ptm, ma_gd, ma_tb, dich_vu, manv_tt_dai, doanhthu_kpi_nvdai 
						from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng')
					union all
						----Gia han VNPts
						select thang, ma_kh, ma_tb, 'VNPTS' dich_vu, ma_nv, doanhthu_dongia 
						from ttkd_bsc.ghtt_vnpts 		--- Nguyen quan ly, chua co vb, toan noi mieng
						where thang=202404 and thang_giao is not null and ma_nv is not null

				)
--				where MANV_PTM in ('VNP017190','VNP027259')
				group by manv_ptm
		;
	create index ttkd_bsc.temp_trasau_canhan_manv on ttkd_bsc.temp_trasau_canhan (manv_ptm)
	;
	--- delete from ttkd_bsc.temp_trasau_canhan where MANV_PTM in ('VNP017190','VNP027259');


--	drop table ttkd_bsc.temp_ptmtt_canhan purge
	;
	
	---Doanh thu VNPtt - Man
--	create table ttkd_bsc.temp_ptmtt_canhan as
--		select manv_ptm, round(sum(doanhthu_kpi_nvptm)/1000000,3) dthu_kpi
--			from ttkd_bsc.vnptt_goi_tonghop_202404 			----Man quan ly tinh, goi do file CT_BSC_PTM
--			where doanhthu_kpi_nvptm is not null and thoadk_bsc=1
--			group by manv_ptm
--		;
--	create index temp_ptmtt_canhan_manv on temp_ptmtt_canhan (manv_ptm)
	;


-- ca nhan
		--update bangluong_kpi_202404_ptm a set hcm_dt_ptmoi_021_ts='', hcm_dt_ptmoi_021_tt=''
		; 
		update ttkd_bsc.bangluong_kpi_202404_ptm a
		   set hcm_dt_ptmoi_021_ts=(select dthu_kpi from ttkd_bsc.temp_trasau_canhan  where manv_ptm=a.ma_nv)
				-- ,hcm_dt_ptmoi_021_tt=(select dthu_kpi from temp_ptmtt_canhan a where manv_ptm=a.ma_nv) 
				
			;
		commit;							 

		update ttkd_bsc.bangluong_kpi_202404_ptm a set tong='', hcm_dt_ptmoi_021=''
		;
		update ttkd_bsc.bangluong_kpi_202404_ptm a
				set tong = nvl(hcm_dt_ptmoi_021_ts,0) + nvl(hcm_dt_ptmoi_021_tt,0)
					   ,hcm_dt_ptmoi_021=nvl(hcm_dt_ptmoi_021_ts,0) + nvl(hcm_dt_ptmoi_021_tt,0)
			-- select ma_nv, ma_vtcv, ten_vtcv, ten_donvi, hcm_dt_ptmoi_021_ts, hcm_dt_ptmoi_021_tt, tong, hcm_dt_ptmoi_021 from ttkd_bsc.bangluong_kpi_202404_ptm a
			where (hcm_dt_ptmoi_021_ts is not null or hcm_dt_ptmoi_021_tt is not null)
--			and  ma_nv in ('VNP017190','VNP027259')
		; 
 

-- to truong: thieu 0021_ts
	drop table ttkd_bsc.temp_totruong purge;
	;
	create table ttkd_bsc.temp_totruong as
			select ma_to, round(sum(doanhthu_kpi_to)/1000000,3) dthu_kpi
			from(
					select ma_to, doanhthu_kpi_to
					  from ttkd_bsc.ct_bsc_ptm a
					  where thang_tlkpi_to=202404 and (loaitb_id<>21 or loaitb_id is null)
				union all
					select ma_to, doanhthu_kpi_dnhm
					  from ttkd_bsc.ct_bsc_ptm a
					 where thang_tlkpi_dnhm_to=202404 and (loaitb_id<>21 or loaitb_id is null)
				union all
					select ma_to, doanhthu_dongia 
					from ttkd_bsc.ghtt_vnpts		----a Nguyen quan ly
					  where thang=202404 and thang_giao is not null and ma_to is not null 
					  
						)group by ma_to
		;


  
		update ttkd_bsc.bangluong_kpi_202404_ptm a
		   set hcm_dt_ptmoi_021_ts=''
		 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi in ('HCM_DT_PTMOI_021') 
												and thang_kt is null and to_truong_pho is not null and ma_vtcv=a.ma_vtcv)
								;
						 
						 
		update ttkd_bsc.bangluong_kpi_202404_ptm a
		   set hcm_dt_ptmoi_021_ts=(select dthu_kpi from ttkd_bsc.temp_totruong where ma_to=a.ma_to)
		 -- select ma_nv, ma_to, ten_donvi, ten_vtcv, hcm_dt_ptmoi_021_ts from bangluong_kpi_202404_ptm a
		 where exists (select dthu_kpi from ttkd_bsc.temp_totruong where ma_to=a.ma_to) 
			and exists(select 1 from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi='HCM_DT_PTMOI_021' and thang_kt is null and to_truong_pho is not null and ma_vtcv=a.ma_vtcv);    
		;
		
		update ttkd_bsc.bangluong_kpi_202404_ptm a
				set tong = nvl(hcm_dt_ptmoi_021_ts,0) + nvl(hcm_dt_ptmoi_021_tt,0)
					   ,hcm_dt_ptmoi_021 = nvl(hcm_dt_ptmoi_021_ts,0) + nvl(hcm_dt_ptmoi_021_tt,0)
			-- select ma_nv, ma_vtcv, ten_vtcv, ten_donvi, hcm_dt_ptmoi_021_ts, hcm_dt_ptmoi_021_tt, tong, hcm_dt_ptmoi_021 from ttkd_bsc.bangluong_kpi_202404_ptm a
			where (hcm_dt_ptmoi_021_ts is not null or hcm_dt_ptmoi_021_tt is not null)
		; 
commit;
---- ldp phu trach:
		drop table ttkd_bsc.temp_021_ldp purge;
		;
		create table ttkd_bsc.temp_021_ldp as  
			select ma_nv, sum(dthu_kpi) dthu_kpi
			from ( select pgd.ma_pb, pgd.ma_nv, pgd.ma_to, sum(bl.dthu_kpi) dthu_kpi
						from (select ma_to, round(sum(doanhthu_kpi_to)/1000000,3) dthu_kpi
									from (select ma_to, doanhthu_kpi_to
												from ttkd_bsc.ct_bsc_ptm a
												where thang_tlkpi_to=202404 and (loaitb_id<>21 or loaitb_id is null)
												union all
												select ma_to, doanhthu_kpi_dnhm
												from ttkd_bsc.ct_bsc_ptm a
												where thang_tlkpi_dnhm_to=202404 and (loaitb_id<>21 or loaitb_id is null)
--												union all
--												select ma_to_ptm, doanhthu_kpi_nvptm 
--												from ttkd_bsc.vnptt_goi_tonghop_202404
--												where doanhthu_kpi_nvptm is not null and thoadk_bsc=1
												union all
												select ma_to, doanhthu_dongia 
												from ttkd_bsc.ghtt_vnpts
												where thang=202404 and thang_giao is not null and ma_to is not null 
											)group by ma_to
								) bl
								, ttkd_bsc.blkpi_dm_to_pgd pgd
						where bl.ma_to=pgd.ma_to and pgd.thang=202404 
							and lower(pgd.ma_kpi) in ('hcm_dt_ptmoi_021')
						group by pgd.ma_pb, pgd.ma_nv, pgd.ma_to     
				) group by ma_nv
			;
                                                                         
   
		update ttkd_bsc.bangluong_kpi_202404_ptm a
		   set hcm_dt_ptmoi_021_ts =''
		 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi='HCM_DT_PTMOI_021' and thang_kt is null and giamdoc_phogiamdoc is not null and ma_vtcv=a.ma_vtcv)
					and exists(select 1 from ttkd_bsc.temp_021_ldp where ma_nv=a.ma_nv)
					;

            
		update ttkd_bsc.bangluong_kpi_202404_ptm a
		   set hcm_dt_ptmoi_021_ts=(select dthu_kpi from ttkd_bsc.temp_021_ldp where ma_nv=a.ma_nv)
		where  exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi='HCM_DT_PTMOI_021' and thang_kt is null and giamdoc_phogiamdoc is not null and ma_vtcv=a.ma_vtcv)
					and exists(select * from ttkd_bsc.temp_021_ldp where ma_nv=a.ma_nv)
					;      
		
		update ttkd_bsc.bangluong_kpi_202404_ptm a
				set tong = nvl(hcm_dt_ptmoi_021_ts,0) + nvl(hcm_dt_ptmoi_021_tt,0)
					   ,hcm_dt_ptmoi_021 = nvl(hcm_dt_ptmoi_021_ts,0) + nvl(hcm_dt_ptmoi_021_tt,0)
			-- select ma_nv, ma_vtcv, ten_vtcv, ten_donvi, hcm_dt_ptmoi_021_ts, hcm_dt_ptmoi_021_tt, tong, hcm_dt_ptmoi_021 from ttkd_bsc.bangluong_kpi_202404_ptm a
			where (hcm_dt_ptmoi_021_ts is not null or hcm_dt_ptmoi_021_tt is not null)
		; 
		
		select * from ttkd_bsc.bangluong_kpi_202404_ptm;
 -_da xong

-- bs cho to truong va ldp chuyen qua tinh thu lao cho kenh ngoai
	-- vanban_id=16 danh cho dai ly ngoai ptm VNPTS goi D500new
	-- nguoi_gt la so eload
	
	----Dieu kien tinh khi co van ban duyet thi moi UPDATE ELOAD --> NV QL ELOAD
	drop table temp_d500new_kenhdaily
	;
	select * from temp_d500new_kenhdaily;
	create table temp_d500new_kenhdaily as;
		select ma_to, 
					(select ma_nv from ttkd_bsc.nhanvien_202404 where (ten_vtcv like 'T_ Tr__ng%' or ten_vtcv like 'C_a H_ng%') and ma_to=a.ma_to) manv_ttg
					,(select ma_nv from ttkd_bsc.blkpi_dm_to_pgd pgd
								  where pgd.thang=202404 and pgd.ma_kpi='HCM_DT_PTMOI_021' and ma_to=a.ma_to) manv_ldp
					,sum(dthu_kpi)/1000000 dthu_kpi_d500
					,(select dt_didong_giao from ttkd_bsc.dinhmuc_dthu_ptm 
						where thang=202404 and dt_didong_giao>0 and (ten_vtcv like 'T_ Tr__ng%' or ten_vtcv like 'C_a H_ng%') and ma_to=a.ma_to) dt_didong_giao_ttg
					,(select round(dt_didong_giao*0.3,6) from ttkd_bsc.dinhmuc_dthu_ptm 
						where thang=202404 and dt_didong_giao>0 and (ten_vtcv like 'T_ Tr__ng%' or ten_vtcv like 'C_a H_ng%') and ma_to=a.ma_to) dthu_kpi_max_ttg
					,cast(null as number) dthu_kpi_bs_ttg
					,cast(null as number) dthu_kpi_d500_ldp
					,cast(null as number) dt_didong_giao_ldp
					,cast(null as number) dthu_kpi_max_ldp
					,cast(null as number) dthu_kpi_bs_ldp
		from (select thang_ptm, ma_gd, ma_tb, dich_vu, ma_to, doanhthu_kpi_nvptm dthu_kpi 
					from ttkd_bsc.ct_bsc_ptm
					where thang_tlkpi_to=202404 and loaitb_id=20 and vanban_id=16            
				union all
					select thang_ptm, ma_gd, ma_tb, dich_vu, ma_to ,doanhthu_kpi_dnhm 
					from ttkd_bsc.ct_bsc_ptm 
						where thang_tlkpi_dnhm_to=202404 and loaitb_id=20 and vanban_id=16    
				) a group by ma_to
	;

		----bo sung KPI to truong 12tr * 30%, lam tron 3 so
		update ttkd_bsc.temp_d500new_kenhdaily a set dthu_kpi_bs_ttg = ''
		;
		update ttkd_bsc.temp_d500new_kenhdaily a
			set dthu_kpi_bs_ttg = (case when dthu_kpi_d500 >= round(12*0.3,3) then round(12*0.3,3)  else dthu_kpi_d500 end)
		;
        
		----bo sung KPI LDP 50tr * 50%, lam tron 3 so
			--> ? KHKT (Tram Anh)
		update ttkd_bsc.temp_d500new_kenhdaily a 
			set dt_didong_giao_ldp= 50.5, 
				   dthu_kpi_max_ldp=round(50.5*0.3,3), 
				   dthu_kpi_d500_ldp=(select dthu_kpi_d500 from ttkd_bsc.temp_d500new_kenhdaily where manv_ldp=a.manv_ldp)
		;
										   
		update ttkd_bsc.temp_d500new_kenhdaily a set dthu_kpi_bs_ldp=
				(case when dthu_kpi_d500_ldp>=dthu_kpi_max_ldp then dthu_kpi_max_ldp else dthu_kpi_d500_ldp end)
		;
        

		alter table ttkd_bsc.bangluong_kpi_202404_ptm add (dthu_kpi_bs number, dthu_kpi_bs_ldp number);
		update ttkd_bsc.bangluong_kpi_202404_ptm a set dthu_kpi_bs='', dthu_kpi_bs_ldp=''
		;
		update ttkd_bsc.bangluong_kpi_202404_ptm a 
				set dthu_kpi_bs=(select dthu_kpi_bs_ttg from ttkd_bsc.temp_d500new_kenhdaily where manv_ttg=a.ma_nv)
			where exists (select DTHU_KPI_BS_TTG from ttkd_bsc.temp_d500new_kenhdaily where manv_ttg=a.ma_nv);


		update ttkd_bsc.bangluong_kpi_202404_ptm a
			set dthu_kpi_bs_ldp = (select distinct dthu_kpi_bs_ldp from ttkd_bsc.temp_d500new_kenhdaily where MANV_LDP=a.ma_nv)
			where exists (select dthu_kpi_bs_ldp from ttkd_bsc.temp_d500new_kenhdaily where MANV_LDP=a.ma_nv);
			
			
		update ttkd_bsc.bangluong_kpi_202404_ptm a 
				set hcm_dt_ptmoi_021=nvl(hcm_dt_ptmoi_021,0)+dthu_kpi_bs
			where dthu_kpi_bs>0
		;


-- update bangluong_kpi_202404:     
		update ttkd_bsc.bangluong_kpi_202404 a 
				set hcm_dt_ptmoi_021='', hcm_dt_ptmoi_021_ts='' 
			where  hcm_dt_ptmoi_021 is not null 
			;    
			
		update ttkd_bsc.bangluong_kpi_202404 a 
				set (hcm_dt_ptmoi_021_ts, hcm_dt_ptmoi_021) = (select hcm_dt_ptmoi_021_ts, hcm_dt_ptmoi_021 
																											from ttkd_bsc.bangluong_kpi_202404_ptm where ma_nv=a.ma_nv) 
			where exists(select 1 from ttkd_bsc.bangluong_kpi_202404_ptm where hcm_dt_ptmoi_021 is not null and ma_nv=a.ma_nv) 
			
			;
					
		update ttkd_bsc.bangluong_kpi_202404 a set hcm_dt_ptmoi_021='' where hcm_dt_ptmoi_021=0; 
		update ttkd_bsc.bangluong_kpi_202404 a set hcm_dt_ptmoi_021_ts='' where hcm_dt_ptmoi_021_ts=0;
			
		commit;    

    
/*
select distinct a.*, c.ten_vtcv
                ,(select count(*) from bangluong_kpi_202404 where ma_vtcv=b.ma_vtcv ) sl_nv
                ,(select count(*) from bangluong_kpi_202404 where hcm_dt_ptmoi_021 is not null and ma_vtcv=b.ma_vtcv ) slnv_dadanhgia
        from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien_202404 c  
        where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv 
                   and a.thang_kt is null and b.thang_kt is null 
                    and lower(a.ma_kpi)='hcm_dt_ptmoi_021' ;
 
                    
select a.ten_donvi, a.ma_to, a.ten_to, a.ma_nv_hrm,  a.ten_nv, a.ma_vtcv, a.ten_vtcv
             ,a.hcm_dt_ptmoi_021 hcm_dt_ptmoi_021_new, b.hcm_dt_ptmoi_021 hcm_dt_ptmoi_021_old,
             nvl(a.hcm_dt_ptmoi_021,0) - nvl(b.hcm_dt_ptmoi_021,0) chechlech
  from bangluong_kpi_202404 a, bangluong_kpi_202404_l5 b 
where a.ma_nv=b.ma_nv 
            and nvl(a.hcm_dt_ptmoi_021,0)<>nvl(b.hcm_dt_ptmoi_021,0)   
order by (a.hcm_dt_ptmoi_021 - b.hcm_dt_ptmoi_021);
                    

*/

------------------------------------------------------------------------------------------------

-- HCM_DT_PTMOI_052 Doanh thu dich vu di dong ptm trong tháng - NEW 03/2023: xet 4 thang nhu _021
select distinct a.*, b.*, b.ten_vtcv from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien_202404 c  
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
					where thang_tlkpi=202404 and loaitb_id=20
				union all
					select thang_ptm, ma_gd, ma_tb, dich_vu, manv_ptm,doanhthu_kpi_dnhm 
					from ttkd_bsc.ct_bsc_ptm 
					where thang_tlkpi_dnhm=202404 and loaitb_id=20
				union all
					select thang_ptm, ma_gd, ma_tb, dich_vu, manv_tt_dai,doanhthu_kpi_nvdai 
					from ttkd_bsc.ct_bsc_ptm 
					where thang_tlkpi=202404 and loaitb_id=20
				union all
					select thang, ma_kh, ma_tb, 'VNPTS', ma_nv, doanhthu_dongia 
					from ttkd_bsc.ghtt_vnpts
						  where thang=202404 and thang_giao is not null and ma_nv is not null 
			)group by manv_ptm
			;
	create index temp_vnpts_manv on temp_vnpts (manv_ptm)
	;


    
	alter table ttkd_bsc.bangluong_kpi_202404_ptm add (hcm_dt_ptmoi_052 number, tong_didong number)
	;
	alter table ttkd_bsc.bangluong_kpi_202404_ptm add didong_trasau number
	;
	update ttkd_bsc.bangluong_kpi_202404_ptm a set hcm_dt_ptmoi_052='', tong_didong='', didong_trasau=''
	;
   
	update ttkd_bsc.bangluong_kpi_202404_ptm a
	   set didong_trasau=(select dthu_kpi from ttkd_bsc.temp_vnpts where manv_ptm=a.ma_nv)
	   ;
                    
 
	update ttkd_bsc.bangluong_kpi_202404_ptm a
	   set tong_didong = nvl(didong_trasau,0) + nvl(hcm_dt_ptmoi_021_tt,0)
	where (didong_trasau is not null or hcm_dt_ptmoi_021_tt is not null)
	; 


-- ca nhan:
	 update ttkd_bsc.bangluong_kpi_202404_ptm a set hcm_dt_ptmoi_052=''
	 ;
	 update ttkd_bsc.bangluong_kpi_202404_ptm a
		  set hcm_dt_ptmoi_052 = tong_didong 
		where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
							where ma_kpi in ('HCM_DT_PTMOI_052')  
									and thang_kt is null and to_truong_pho is null and giamdoc_phogiamdoc is null and ma_vtcv=a.ma_vtcv)
	;
                commit;
                
-- kpi to truong: 
	drop table ttkd_bsc.temp_didong_totruong purge;
	create table ttkd_bsc.temp_didong_totruong as
				select ma_to, sum(didong_trasau) dthu_kpi--, sum(dthu_kpi_bs) dthu_kpi_bs 
				from ttkd_bsc.bangluong_kpi_202404_ptm a
				group by ma_to;

                            
		update ttkd_bsc.bangluong_kpi_202404_ptm a
			set hcm_dt_ptmoi_052=(select nvl(dthu_kpi,0) from ttkd_bsc.temp_didong_totruong where ma_to=a.ma_to) + nvl(hcm_dt_ptmoi_021_tt,0)
		 -- select ma_nv, ma_to, ma_vtcv, ten_vtcv, hcm_dt_ptmoi_052, hcm_dt_ptm_052_ttg from bangluong_kpi_202404_ptm a
		 where exists (select 1 from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi in ('HCM_DT_PTMOI_052') and thang_kt is null and to_truong_pho is not null and ma_vtcv=a.ma_vtcv)
					-- and exists (select 1 from temp_didong_totruong where ma_to=a.ma_to)
					;
commit;
                                    
---- ldp phu trach:
		drop table ttkd_bsc.temp_052_ldp purge;
		create table ttkd_bsc.temp_052_ldp as  
		select pgd.ma_pb, pgd.ma_nv, bl.ten_donvi, (select ten_nv from ttkd_bsc.nhanvien_202404 where ma_nv=pgd.ma_nv) ten_nv, 
				round(sum(bl.didong_trasau) ,3) dthu_kpi--, sum(bl.dthu_kpi_bs) dthu_kpi_bs
		   from ttkd_bsc.bangluong_kpi_202404_ptm bl,  ttkd_bsc.blkpi_dm_to_pgd pgd
			where bl.ma_to=pgd.ma_to and pgd.thang=202404 and pgd.ma_kpi in ('HCM_DT_PTMOI_052')
						--and not exists (select 1 from nv_thuviec where thang=202404 and ma_to=pgd.ma_to and ma_nv=bl.ma_nv)
			group by pgd.ma_pb, pgd.ma_nv, bl.ten_donvi;
																						

		alter table ttkd_bsc.bangluong_kpi_202404_ptm add hcm_dt_ptmoi_052_ldp number;
		update ttkd_bsc.bangluong_kpi_202404_ptm a set hcm_dt_ptmoi_052_ldp='';
			 
		update ttkd_bsc.bangluong_kpi_202404_ptm a
		   set hcm_dt_ptmoi_052='', hcm_dt_ptmoi_052_ldp=''
		 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi in ('HCM_DT_PTMOI_052') 
											and thang_kt is null and giamdoc_phogiamdoc is not null and ma_vtcv=a.ma_vtcv)
					and exists(select 1 from ttkd_bsc.temp_052_ldp where ma_nv=a.ma_nv);
					
					
		update ttkd_bsc.bangluong_kpi_202404_ptm a
		   set hcm_dt_ptmoi_052=(select nvl(dthu_kpi,0) from ttkd_bsc.temp_052_ldp where ma_nv=a.ma_nv) + nvl(hcm_dt_ptmoi_021_tt,0)
			--	 hcm_dt_ptmoi_052_ldp=(select nvl(dthu_kpi,0)+nvl(dthu_kpi_bs,0) from temp_052_ldp where ma_nv=a.ma_nv)
		-- select * from bangluong_kpi_202404_ptm a
		where  exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									where ma_kpi in ('HCM_DT_PTMOI_052') and thang_kt is null and giamdoc_phogiamdoc is not null and ma_vtcv=a.ma_vtcv)
					and exists (select 1 from ttkd_bsc.blkpi_dm_to_pgd pgd
										 where pgd.thang=202404 and pgd.ma_kpi in ('HCM_DT_PTMOI_052') and ma_nv=a.ma_nv)
					and exists(select * from ttkd_bsc.temp_052_ldp where ma_nv=a.ma_nv)
				;      
		 
		 
		 commit;
 
-- update bangluong_kpi_202404 
		update ttkd_bsc.bangluong_kpi_202404 a set hcm_dt_ptmoi_052='';     
		update ttkd_bsc.bangluong_kpi_202404 a set hcm_dt_ptmoi_052=
				(select hcm_dt_ptmoi_052 from ttkd_bsc.bangluong_kpi_202404_ptm where hcm_dt_ptmoi_052>0 and ma_nv=a.ma_nv) 
			-- select hcm_dt_ptmoi_052, hcm_dt_ptmoi_021 from bangluong_kpi_202404 a
			where exists(select hcm_dt_ptmoi_052 from ttkd_bsc.bangluong_kpi_202404_ptm where hcm_dt_ptmoi_052>0 and ma_nv=a.ma_nv) ;
				
		commit;  
		---da xong
		select * from ttkd_bsc.bangluong_kpi_202404_ptm;

			select distinct a.*, b.*, c.ten_vtcv 
							,(select count(*) from ttkd_bsc.bangluong_kpi_202404 where ma_vtcv=b.ma_vtcv ) slnv
							,(select count(*) from ttkd_bsc.bangluong_kpi_202404 where hcm_dt_ptmoi_052 is not null and ma_vtcv=b.ma_vtcv ) slnv_dadanhgia
					from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien_202404 c  
					where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv 
							   and a.thang_kt is null and b.thang_kt is null 
								and lower(a.ma_kpi)='hcm_dt_ptmoi_052' ;
			  
								
			select a.ten_donvi, a.ten_to, a.ma_nv,  a.ten_nv, a.ma_vtcv, a.ten_vtcv
						 ,a.hcm_dt_ptmoi_052 hcm_dt_ptmoi_052_new, b.hcm_dt_ptmoi_052 hcm_dt_ptmoi_052_old,
						 nvl(a.hcm_dt_ptmoi_052,0) - nvl(b.hcm_dt_ptmoi_052,0) chechlech
			  from ttkd_bsc.bangluong_kpi_202404 a, ttkd_bsc.bangluong_kpi_202404_l3 b 
			where a.ma_nv=b.ma_nv 
						and nvl(a.hcm_dt_ptmoi_052,0)<>nvl(b.hcm_dt_ptmoi_052,0)    
			order by (a.hcm_dt_ptmoi_052 - b.hcm_dt_ptmoi_052);
				

			select a.ten_donvi, a.ten_to, a.ma_nv,  a.ten_nv, a.ten_vtcv
						 ,a.hcm_dt_ptmoi_052 new, b.hcm_dt_ptmoi_052 old,
						 nvl(a.hcm_dt_ptmoi_052,0) - nvl(b.hcm_dt_ptmoi_052,0) chechlech
			  from bangluong_kpi_202404 a, bangluong_kpi_202404_l2 b 
			where a.ma_nv=b.ma_nv 
						and nvl(a.hcm_dt_ptmoi_052,0)<>nvl(b.hcm_dt_ptmoi_052,0)    
			order by (a.hcm_dt_ptmoi_052 - b.hcm_dt_ptmoi_052)
			;


--- DTHU CNTT:
/*-- Kiem tra danh muc loaitb_id thieu trong danh muc
insert into ttkd_bsc.dm_loaihinh_hsqd (DICHVUVT_ID, LOAITB_ID, LOAIHINH_TB)
    select distinct dichvuvt_id,loaitb_id, dich_vu --, (select loaihinh_tb from css_hcm.loaihinh_tb@ttkddb where loaitb_id=a.loaitb_id)loaihinh_tb
    from ttkd_bsc.ct_bsc_ptm a
    where thang_ptm=202404
        and not exists(select * from ttkd_bsc.dm_loaihinh_hsqd where loaitb_id=a.loaitb_id);
        
select distinct b.loaitb_id, b.loaihinh_tb, b.dv_cap2
  from ct_bsc_ptm a, ttkd_bsc.dm_loaihinh_hsqd b
    where b.loaitb_id<>21 and a.loaitb_id=b.loaitb_id and a.thang_tlkpi=202404 and b.dv_cap2 is null;

*/

	select HCM_DT_PTMOI_044 
	from ttkd_bsc.bangluong_kpi_202404 a
    where HCM_DT_PTMOI_044 is not null
			and exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
						where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and giamdoc_phogiamdoc is not null
						  and ma_vtcv=a.ma_vtcv
						 )
					  ;
                      
                      
--TT = tat ca NV trong to khong loai tru thu viec hay CTV
		select loaitb_id from ttkd_bsc.ct_bsc_ptm a
		where thang_tlkpi=202404 and dichvuvt_id in (13,14,15,16) 
				and exists(select * from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 is null and loaitb_id=a.loaitb_id)
				;


----------- Ca nhan 
		drop table ttkd_bsc.temp_cntt purge;
		;
		create table ttkd_bsc.temp_cntt as
				select manv_ptm ma_nv,sum(doanhthu_kpi_nvptm)dthu
				  from(
						select manv_ptm,doanhthu_kpi_nvptm
						  from ttkd_bsc.ct_bsc_ptm a
						 where thang_tlkpi=202404
						   and exists(select * from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('CNTT','Ha tang CNTT') and loaitb_id=a.loaitb_id)
					union all
						select manv_ptm,doanhthu_kpi_dnhm
						  from ttkd_bsc.ct_bsc_ptm a
						 where thang_tlkpi_dnhm=202404
						   and exists(select * from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('CNTT','Ha tang CNTT') and loaitb_id=a.loaitb_id) 
				)group by manv_ptm
		;
 
-- Kiem tra:
		select distinct loaitb_id from ct_bsc_ptm where thang_tlkpi=202404; and loaitb_id is null
		;


		update ttkd_bsc.bangluong_kpi_202404 a
		   set HCM_DT_PTMOI_044=''
		 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
						where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and to_truong_pho is null and giamdoc_phogiamdoc is null
						  and ma_vtcv=a.ma_vtcv)
			;
			  
		update ttkd_bsc.bangluong_kpi_202404 a
		   set HCM_DT_PTMOI_044=(select round(dthu/1000000,3) from ttkd_bsc.temp_cntt where ma_nv=a.ma_nv_hrm) 
		 where exists(select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
					   where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and to_truong_pho is null and giamdoc_phogiamdoc is null
						 and ma_vtcv=a.ma_vtcv)
			;


		select a.ma_nv, a.ten_vtcv, a.ten_donvi, HCM_DT_PTMOI_044 from ttkd_bsc.bangluong_kpi_202404 a
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
							 where thang_tlkpi_to=202404
							   and exists(select * from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('CNTT','Ha tang CNTT') and loaitb_id=a.loaitb_id)
							 union all
							select ma_to, doanhthu_kpi_dnhm_phong
							  from ttkd_bsc.ct_bsc_ptm a
							 where thang_tlkpi_dnhm_to=202404
							   and exists(select * from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('CNTT','Ha tang CNTT') and loaitb_id=a.loaitb_id) 
			)group by ma_to
			;
			 

			update ttkd_bsc.bangluong_kpi_202404 a
			   set HCM_DT_PTMOI_044=''
			 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
							where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and to_truong_pho is not null
							  and ma_vtcv=a.ma_vtcv);
				  
			update ttkd_bsc.bangluong_kpi_202404 a
			   set HCM_DT_PTMOI_044=(select round(dthu/1000000,3) from ttkd_bsc.temp_cntt where ma_to=a.ma_to) 
			 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
							where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and to_truong_pho is not null
							  and ma_vtcv=a.ma_vtcv)
							  ;
							  commit;

			select a.ma_nv, a.ten_vtcv, a.ten_donvi, HCM_DT_PTMOI_044 
			from  ttkd_bsc.bangluong_kpi_202404 a
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
								 where thang_tlkpi_to=202404 -- thang_tlkpi_phong=202404
								   and exists(select * from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('CNTT','Ha tang CNTT') and loaitb_id=a.loaitb_id)
								 union all
								select ma_to,doanhthu_kpi_dnhm_phong
								  from ttkd_bsc.ct_bsc_ptm a
								 where thang_tlkpi_dnhm_to=202404 -- thang_tlkpi_dnhm_phong=202404
								   and exists(select * from ttkd_bsc.dm_loaihinh_hsqd where dv_cap2 in ('CNTT','Ha tang CNTT') and loaitb_id=a.loaitb_id)    
							   )
						  group by ma_to)x
					  ,(select ma_nv manv_pgd,ma_to from ttkd_bsc.blkpi_dm_to_pgd where thang=202404 and upper(ma_kpi)='HCM_DT_PTMOI_044')y
				 where x.ma_to=y.ma_to
		;
		 
		 
		 
		update ttkd_bsc.bangluong_kpi_202404 a
		   set HCM_DT_PTMOI_044=''
		 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
						where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and giamdoc_phogiamdoc is not null
						  and ma_vtcv=a.ma_vtcv);
			  
		update ttkd_bsc.bangluong_kpi_202404 a
		   set HCM_DT_PTMOI_044=(select round(sum(dthu)/1000000,3) from ttkd_bsc.temp_cntt where manv_pgd=a.ma_nv) 
		 where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv 
						where thang_kt is null and upper(ma_kpi)='HCM_DT_PTMOI_044' and giamdoc_phogiamdoc is not null
						  and ma_vtcv=a.ma_vtcv);
						  
		commit;                  

---END HCM_DT_PTMOI_044

		 select distinct a.*, b.ma_vtcv, c.ten_vtcv
						,(select count(*) from ttkd_bsc.bangluong_kpi_202404 
								where hcm_dt_ptmoi_044 is not null and ma_vtcv=b.ma_vtcv ) slnv_dadanhgia
				from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien_202404 c  
				where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv 
						   and a.thang_kt is null and b.thang_kt is null 
							and lower(a.ma_kpi)='hcm_dt_ptmoi_044' ;
		 
		 
		select a.ten_donvi, a.ten_to, a.ma_nv_hrm,  a.ten_nv, a.ma_vtcv, a.ten_vtcv
					 ,a.hcm_dt_ptmoi_044 new, b.hcm_dt_ptmoi_044 old,
					 nvl(a.hcm_dt_ptmoi_044,0) - nvl(b.hcm_dt_ptmoi_044,0) chechlech
		  from ttkd_bsc.bangluong_kpi_202404 a, ttkd_bsc.bangluong_kpi_202404_l7 b 
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
							
		-- HCM_DT_SSHOP_002: Doanh thu bán hàng qua DigiShop = Doanh thu qui doi thuc hien / Doanh thu giao trong thang 
		select distinct a.*, b.*, b.ten_vtcv from blkpi_danhmuc_kpi a, blkpi_danhmuc_kpi_vtcv b, nhanvien_202402 c  
				where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv       
							and a.thang_kt is null and b.thang_kt is null 
							and lower(a.ma_kpi)='hcm_dt_sshop_002' 
		;
                    

delete from ct_ptm_shop where thang=202402;
insert into ct_ptm_shop
select  202402 thang, donhang, ma_gd, ma_tb, loaihinh_tb, ten_kh, diachi, tien_hoamang, doanhthu dthu_goi, tongdt, goidangky
            ,mahrm ma_nv, nhanvien, (select ma_to from ttkd_bsc.nhanvien_202402 where ma_nv=a.mahrm) ma_to
            ,(select ten_to from ttkd_bsc.nhanvien_202402 where ma_nv=a.mahrm) ten_to
            ,(select ma_pb from ttkd_bsc.nhanvien_202402 where ma_nv=a.mahrm) ma_pb
           ,(select ten_pb from ttkd_bsc.nhanvien_202402 where ma_nv=a.mahrm) ten_pb
           ,'hcm_br_online_202402'
    from dulieu_ftp.hcm_br_online_202402@vinadata a
    where tinhban='HCM' and ma_tinh='HCM'

union all
/*
select 202402 thang, donhang, magd ma_gd, '' ma_tb, 'MyTV' loaihinh_tb, tenkh ten_kh, diachi
            ,cast(null as number) tien_hoamang, cast(null as number) dthu_goi, tongdt, goidk goidangky
            ,mahrm ma_nv, nhanvien, (select ma_to from ttkd_bsc.nhanvien_202402 where ma_nv=a.mahrm) ma_to
            ,(select ten_to from ttkd_bsc.nhanvien_202402 where ma_nv=a.mahrm) ten_to
            ,(select ma_pb from ttkd_bsc.nhanvien_202402 where ma_nv=a.mahrm) ma_pb
            ,(select ten_pb from ttkd_bsc.nhanvien_202402 where ma_nv=a.mahrm) ten_pb
            ,'hcm_mytv_online_202402'
    from dulieu_ftp.hcm_mytv_online_202402@vinadata a
    where tinhban='HCM' and ma_tinh='HCM'

union all
*/
select 202402 thang, donhang, cast(null as varchar2(30)) ma_gd, cast(null as varchar2(30)) ma_tb, 'Sim so - '||hinhthuc loaihinh_tb, 
            cast(null as varchar2(100)) ten_kh, cast(null as varchar2(100)) diachi, dt_sim tien_hoamang, dt_goi dthu_goi, nvl(dt_sim,0)+nvl(dt_goi,0) tongdt, package goidangky
            ,mahrm ma_nv, nhanvien, (select ma_to from ttkd_bsc.nhanvien_202402 where ma_nv=a.mahrm) ma_to
            ,(select ten_to from ttkd_bsc.nhanvien_202402 where ma_nv=a.mahrm) ten_to
            ,(select ma_pb from ttkd_bsc.nhanvien_202402 where ma_nv=a.mahrm) ma_pb
            ,(select ten_pb from ttkd_bsc.nhanvien_202402 where ma_nv=a.mahrm) ten_pb
            ,'hcm_sim_online_202402'
    from dulieu_ftp.hcm_sim_online_202402@vinadata a
    where tinhban='HCM' and ma_tinh='HCM'
    
union all   
select 202402 thang, donhang, cast(null as varchar2(30)) ma_gd, stb ma_tb, 'Goi le' loaihinh_tb, 
            cast(null as varchar2(100)) ten_kh, cast(null as varchar2(100)) diachi, cast(null as number) tien_hoamang, 
            doanhthu dthu_goi, doanhthu tongdt, tengoi goidangky
            ,mahrm ma_nv, nhanvien, (select ma_to from ttkd_bsc.nhanvien_202402 where ma_nv=a.mahrm) ma_to
            ,(select ten_to from ttkd_bsc.nhanvien_202402 where ma_nv=a.mahrm) ten_to
            ,(select ma_pb from ttkd_bsc.nhanvien_202402 where ma_nv=a.mahrm) ma_pb
            ,(select ten_pb from ttkd_bsc.nhanvien_202402 where ma_nv=a.mahrm) ten_pb
            ,'hcm_pac_online_202402'
    from dulieu_ftp.hcm_pac_online_202402@vinadata a
    where tinhban='HCM' and ma_tinh='HCM';    


-- nhanvien:
update bangluong_kpi_202402 a 
    set hcm_dt_sshop_002=''
    where exists (select * from blkpi_danhmuc_kpi_vtcv
                where thang_kt is null and lower(ma_kpi)='hcm_dt_sshop_002' and to_truong_pho is null
                            and ma_vtcv=a.ma_vtcv);
                          
                          
update bangluong_kpi_202402 a
   set hcm_dt_sshop_002=(select round(sum(tongdt)/1000000 ,3) from ttkd_bsc.ct_ptm_shop where thang=202402 and ma_nv=a.ma_nv)
  -- select ma_nv, ten_nv, ten_vtcv, ten_to, hcm_dt_sshop_002 from bangluong_kpi_202402 a
  where exists (select * from blkpi_danhmuc_kpi_vtcv
                where thang_kt is null and lower(ma_kpi)='hcm_dt_sshop_002' and to_truong_pho is null
                            and ma_vtcv=a.ma_vtcv)
        and exists(select 1 from ttkd_bsc.ct_ptm_shop where thang=202402 and ma_nv=a.ma_nv);

 
-- to truong:
update bangluong_kpi_202402 a 
    set hcm_dt_sshop_002=''
    where exists (select * from blkpi_danhmuc_kpi_vtcv
                where thang_kt is null and lower(ma_kpi)='hcm_dt_sshop_002' and to_truong_pho is not null and ma_vtcv=a.ma_vtcv);
                            
                            
update bangluong_kpi_202402 a
   set hcm_dt_sshop_002=(select round(sum(tongdt)/1000000 ,3) from ttkd_bsc.ct_ptm_shop where thang=202402 and ma_to=a.ma_to)
  -- select ma_nv, ten_nv, ten_vtcv, hcm_dt_sshop_002 from bangluong_kpi_202402 a
  where exists (select * from blkpi_danhmuc_kpi_vtcv
                where thang_kt is null and lower(ma_kpi)='hcm_dt_sshop_002' and to_truong_pho is not null
                            and ma_vtcv=a.ma_vtcv);   

commit;


select ma_nv, ten_nv, ten_vtcv, hcm_dt_sshop_002 from bangluong_kpi_202402 a
 where exists (select * from blkpi_danhmuc_kpi_vtcv
                where thang_kt is null and lower(ma_kpi)='hcm_dt_sshop_002' 
                            and ma_vtcv=a.ma_vtcv);          
                            


/*--------------------------------------------------------------------------------------

select ma_nv, hcm_dt_ptmoi_021 from bangluong_kpi_202404;

select * from bangluong_kpi_202404;
select a.ten_donvi, a.ten_to, a.ma_nv, a.ten_nv, a.ten_vtcv,a.ma_nv_hrm, a.hcm_dt_ptmoi_021_ts , a.hcm_dt_ptmoi_021_tt , a.hcm_dt_ptmoi_021
from bangluong_kpi_202404 a
    where a.hcm_dt_ptmoi_021 is not null;

select sum(hcm_dt_ptmoi_021_ts)+sum(hcm_dt_ptmoi_021_tt)  from bangluong_kpi_202404 
union all
select sum(hcm_dt_ptmoi_021) from bangluong_kpi_202404 ;


-- gui PNS:
-- ptm:
select a.ten_donvi, a.ma_to, a.ten_to, a.ma_nv, a.ma_vtcv, a.ten_nv, a.ten_vtcv, a.ma_nv_hrm   ,        
             a.hcm_dt_ptmoi_021 hcm_dt_ptmoi_021_new, 
             b.hcm_dt_ptmoi_021 hcm_dt_ptmoi_021_old,
            nvl(a.hcm_dt_ptmoi_021,0)-nvl(b.hcm_dt_ptmoi_021,0) chechlech

    from bangluong_kpi_202404 a, bangluong_kpi_202404_l4 b 
    where a.ma_nv=b.ma_nv and nvl(a.hcm_dt_ptmoi_021,0)<>nvl(b.hcm_dt_ptmoi_021,0)

order by (a.hcm_dt_ptmoi_021 - b.hcm_dt_ptmoi_021);

-- hcm_dt_ptmoi_052
select a.ten_donvi, a.ma_to, a.ten_to, a.ma_nv, a.ma_vtcv, a.ten_nv, a.ten_vtcv, a.ma_nv_hrm   ,        
             a.hcm_dt_ptmoi_052 hcm_dt_ptmoi_052_new, 
             b.hcm_dt_ptmoi_052 hcm_dt_ptmoi_052_old,
            nvl(a.hcm_dt_ptmoi_052,0)-nvl(b.hcm_dt_ptmoi_052,0) chechlech

    from bangluong_kpi_202404 a, bangluong_kpi_202404_l21 b 
    where a.ma_nv=b.ma_nv and nvl(a.hcm_dt_ptmoi_052,0)<>nvl(b.hcm_dt_ptmoi_052,0)

order by (a.hcm_dt_ptmoi_052 - b.hcm_dt_ptmoi_052);


-- DT CNTT:
create table bangluong_kpi_202404_l7 as select * from bangluong_kpi_202404;
select a.ten_donvi, a.ten_to, a.ma_nv, a.ten_nv, a.ten_vtcv, a.ma_nv_hrm   
             ,a.hcm_dt_ptmoi_044 hcm_dt_ptmoi_044_new,
             b.hcm_dt_ptmoi_044 hcm_dt_ptmoi_044_old,
            nvl(a.hcm_dt_ptmoi_044,0)-nvl(b.hcm_dt_ptmoi_044,0) chechlech

    from bangluong_kpi_202404 a, bangluong_kpi_202404_l7 b 
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

    from bangluong_kpi_202404 a, bangluong_kpi_202404_l4 b 
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
    from bangluong_kpi_202404 a, bangluong_kpi_202404_l2 b 
    where a.ma_nv=b.ma_nv and nvl(a.hcm_ct_cluoc_001,0)<>nvl(b.hcm_ct_cluoc_001,0)
order by (a.hcm_ct_cluoc_001 - b.hcm_ct_cluoc_001);


-- C2:
select a.ten_donvi, a.ten_to, a.ma_nv, a.ten_nv, a.ten_vtcv, a.ma_nv_hrm   
             ,a.HCM_CT_CLUOC_001 HCM_CT_CLUOC_001_new,
             b.HCM_CT_CLUOC_001 HCM_CT_CLUOC_001_old,
            nvl(a.HCM_CT_CLUOC_001,0)-nvl(b.HCM_CT_CLUOC_001,0) chechlech

    from bangluong_kpi_202404 a, temp_dc b 
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
    from bangluong_kpi_202404 a, bangluong_kpi_202404_l2 b 
    where a.ma_nv=b.ma_nv and nvl(a.hcm_tb_giaha_023,0)<>nvl(b.hcm_tb_giaha_023,0)
order by (a.hcm_tb_giaha_023 - b.hcm_tb_giaha_023);


select a.ten_donvi, a.ma_to, a.ten_to, a.ma_nv, a.ma_vtcv, a.ten_nv, a.ten_vtcv, a.ma_nv_hrm   ,        
             a.hcm_tb_giaha_022 hcm_tb_giaha_022_new, 
             b.hcm_tb_giaha_022 hcm_tb_giaha_022_old,
            nvl(a.hcm_tb_giaha_022,0)-nvl(b.hcm_tb_giaha_022,0) chechlech
    from bangluong_kpi_202404 a, bangluong_kpi_202404_l2 b 
    where a.ma_nv=b.ma_nv and nvl(a.hcm_tb_giaha_022,0)<>nvl(b.hcm_tb_giaha_022,0)
order by (a.hcm_tb_giaha_022 - b.hcm_tb_giaha_022);


select * from bangluong_kpi_202404;
*/