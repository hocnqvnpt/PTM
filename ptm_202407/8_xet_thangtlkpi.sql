update ttkd_bsc.ct_bsc_ptm a
		    set 
					thang_tlkpi = thang_tldg_dt
					,thang_tlkpi_to = thang_tldg_dt
					,thang_tlkpi_phong = thang_tldg_dt
--		       select thang_luong, thang_ptm, ma_tb, nguon, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, thang_tldg_dt, doanhthu_kpi_nvptm, doanhthu_kpi_to, doanhthu_kpi_phong, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm  a
		    where (loaitb_id not in (21) or ma_kh='GTGT rieng') 
						and nvl(thang_tlkpi, 0) <> 999999		---cac th ngoai le khong tinh KPI, chi tinh DONGIA
						and nvl(thang_tlkpi, 999999) >= 202407
						and thang_tldg_dt = 202407
--						and thang_tlkpi is null
--						and thang_luong = 86
						
		 --   and ma_tb = 'hcm_ca_00067906'
		 
		    ;
		    commit;


		update ttkd_bsc.ct_bsc_ptm a
		    set thang_tlkpi_hotro = thang_tldg_dt_nvhotro
		    
		--     select thang_luong, thang_ptm, ma_tb, nguon, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, thang_tldg_dt, thang_tldg_dt_nvhotro, thang_tlkpi_hotro, lydo_khongtinh_dongia, nop_du, mien_hsgoc, manv_ptm, manv_hotro from ttkd_bsc.ct_bsc_ptm  a
		    where (loaitb_id not in (21) or ma_kh='GTGT rieng') 
						and nvl(thang_tlkpi_hotro, 0) <> 999999
						and nvl(thang_tlkpi_hotro, 999999) >= 202407
						and thang_tldg_dt_nvhotro = 202407
		--				and thang_luong = 86
		
		;
	
    commit;


update ttkd_bsc.ct_bsc_ptm a
   set 
		thang_tlkpi_dnhm = thang_tldg_dnhm
		 , thang_tlkpi_dnhm_to=thang_tldg_dnhm
		 , thang_tlkpi_dnhm_phong=thang_tldg_dnhm
--     select thang_luong, thang_ptm,ma_tb, nguon, thang_tldg_dnhm, thang_tlkpi_dnhm, thang_tlkpi_dnhm_to, thang_tlkpi_dnhm_phong, doanhthu_kpi_dnhm, lydo_khongtinh_dongia, nop_du, mien_hsgoc, manv_ptm, manv_hotro from ttkd_bsc.ct_bsc_ptm  a   
		   where (loaitb_id not in (21) or ma_kh='GTGT rieng') 
						and nvl(thang_tlkpi_dnhm, 0) <> 999999
						and nvl(thang_tlkpi_dnhm, 999999) >= 202407
						and thang_tldg_dnhm = 202407 
						and nvl(thang_tlkpi_dnhm_phong, 999999) >= 202407
						
--						and thang_luong = 86
		;
   rollback;
   commit;
-- Ky n 
-- to: 
-- DL_CNT  - DT cua AM QLDL (DN1) ko ghi nhan cho Truong line ngoai tru Dai ly thu tuc CNT
				-- DT cua AM QLDL (DN2, 3) ghi nhan cho Truong line vi khong co To QLDL
		update ttkd_bsc.ct_bsc_ptm a
		   set thang_tlkpi_to = thang_ptm
--		  select chuquan_id, ma_tb, ma_nguoigt, ten_pb, ma_to, manv_ptm, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, lydo_khongtinh_luong,lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
		 where thang_ptm = 202407
					   and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') and chuquan_id in (145,266)
					   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
					   and (upper(ma_tiepthi_new) in ('DL_CNT','GTGT00001') or upper(nguoi_gt) in ('DL_CNT','GTGT00001') or upper(ma_nguoigt) in ('DL_CNT','GTGT00001'))   
					   and (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id=7) or loaitb_id in (89,90,146) )
					   and (nop_du=1 or mien_hsgoc is not null) 
					   and dthu_ps is not null and nocuoc_ptm is null
					   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
					   and thang_tlkpi_to is null 
					  
			;
   

-- to - dnhm DL_CNT
--update ttkd_bsc.ct_bsc_ptm set thang_tlkpi_dnhm_to='' where thang_ptm=202404
--;

			update ttkd_bsc.ct_bsc_ptm a  
			   set thang_tlkpi_dnhm_to=thang_ptm
--			  select ma_tb, thang_tldg_dt, thang_tlkpi_dnhm, thang_tlkpi_dnhm_to, thang_tlkpi_dnhm_phong, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			  where thang_ptm=202407 and thang_tlkpi_dnhm_to is null and loaitb_id<>21
				  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
				  and  (upper(ma_tiepthi_new) in ('DL_CNT','GTGT00001') or upper(nguoi_gt) in ('DL_CNT','GTGT00001') or upper(ma_nguoigt) in ('DL_CNT','GTGT00001'))   
				  and  (tien_dnhm>0 or tien_sodep>0) 
				  and ((loaitb_id!=20 and trangthai_tt_id=1) or loaitb_id=20)
				  and (nop_du=1 or mien_hsgoc is not null) 
				  and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
				 
				  ;


   
  -- phong - Dai ly
		update ttkd_bsc.ct_bsc_ptm a 
		   set thang_tlkpi_phong = thang_ptm
--		    select thang_ptm, heso_daily, ma_tb, ten_pb, ma_tiepthi, ma_to, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, lydo_khongtinh_luong, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			where thang_ptm = 202407 and thang_tlkpi_phong is null 
					   and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') and chuquan_id in (145,266)
					   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
					   and lydo_khongtinh_luong like '%Phat trien qua Dai ly%'
					   and (nop_du=1 or mien_hsgoc is not null) 
					   and (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id=7) or loaitb_id in (89,90,146) )
					   and dthu_ps is not null and nocuoc_ptm is null 
					   and ((loaitb_id!=20 and trangthai_tt_id=1) or loaitb_id=20)
					   
		   ;

		---ngung chay tu thang 7, moi thang ktra xem de biet
-- nv, to, phong - kenh CTVXHH:    ---tinh theo nvien hotro
		update ttkd_bsc.ct_bsc_ptm a 
		   set thang_tlkpi = a.thang_ptm, thang_tlkpi_to = a.thang_ptm, thang_tlkpi_phong = a.thang_ptm
--		     select thang_ptm, ungdung_id, ma_tb, ten_pb, ma_tiepthi, loai_ld , ma_to, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, lydo_khongtinh_luong, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			where thang_ptm=202407 and thang_tlkpi is null 
						   and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') and chuquan_id in (145,266)
						   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
						   and (nop_du=1 or mien_hsgoc is not null) 
						   and (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id=7) or loaitb_id in (89,90,146) )
						   and dthu_ps>0 and nocuoc_ptm is null
						   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149)) 
--						   and ungdung_id=17 --and a.BS_LUUKHO = 20240718

		   ;
commit;

-- dnhm - phong:  dai ly 
			update ttkd_bsc.ct_bsc_ptm a
			   set thang_tlkpi_dnhm_phong=thang_ptm
--			  select ma_tb, thang_tlkpi_dnhm_phong, thang_tlkpi_dnhm, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			 where thang_ptm = 202407 and thang_tlkpi_dnhm_phong is null
			   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
			   and lydo_khongtinh_luong like '%Phat trien qua Dai ly%' 
			   and (nop_du=1 or mien_hsgoc is not null) 
			   and (tien_dnhm>0 or tien_sodep>0) 
			   and dthu_ps is not null and nocuoc_ptm is null
			   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149)) 
			   and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
			   
			   ;
 commit;
 
		---khong chy tu T7, theo doi
-- dnhm - nv/to/phong - kenh CTVXHH	----theo doi hok co T6
				update ttkd_bsc.ct_bsc_ptm a
				   set thang_tlkpi_dnhm = 202407, thang_tlkpi_dnhm_to=202407, thang_tlkpi_dnhm_phong=202407
--				  select loai_ld, ma_tb, thang_tlkpi_dnhm_phong, thang_tlkpi_dnhm, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
				 where thang_ptm = 202407 and thang_tlkpi=202407 and thang_tlkpi_dnhm_phong is null 
							   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id) 
							   and (nop_du=1 or mien_hsgoc is not null) 
							   and (tien_dnhm>0 or tien_sodep>0) 
							   and dthu_ps is not null and nocuoc_ptm is null
							   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
							   and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
							   and ungdung_id=17
				   ;
				   
				   	--Chi 1 thang 7 theo eo VANBAN_ID
				update ttkd_bsc.ct_bsc_ptm a  
								set THANG_TLDG_DT_DAI = thang_tldg_dt
										, thang_tlkpi_dnhm = null
										, ghichu = thang_luong || '; ' || ghichu
--						select * from ttkd_bsc.ct_bsc_ptm a
						where thang_luong = 71
						
				;
 
 rollback;
-- Ky (n-1)  
  -- to: 
				update ttkd_bsc.ct_bsc_ptm a
				   set thang_tlkpi_to = 202407
--				 select ma_tb, dich_vu, lydo_khongtinh_luong, lydo_khongtinh_dongia, thang_tlkpi, thang_tlkpi_to, thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a
				 where thang_ptm = 202406 and thang_tlkpi_to is null 
						   and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in (145,266)
						   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
						   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
						   and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%'
										and (upper(ma_tiepthi_new) in ('DL_CNT','GTGT00001') or upper(nguoi_gt) in ('DL_CNT','GTGT00001') or upper(ma_nguoigt) in ('DL_CNT','GTGT00001'))))
						   and (trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) 
											or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n1=7) or loaitb_id in (89,90,146) 
									)
						   and ( nop_du=1 or mien_hsgoc is not null)
						   and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 and nocuoc_n1 is null
						   
				   ;


-- phong - dai ly --- theo doi khong co du lieu: T6
			update ttkd_bsc.ct_bsc_ptm a 
			   set thang_tlkpi_phong = 202407
--			 select thang_tldg_dt, thang_tlkpi_phong, lydo_khongtinh_luong, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			 where thang_ptm=202406 and thang_tlkpi_phong is null 
						   and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in (145,266)
						   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
						   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
						   and (lydo_khongtinh_luong is null or lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
						   and ( ( (ngay_luuhs_ttkd is not null or ngay_luuhs_ttvt is not null or ngay_scan is not null) and nop_du=1)  or mien_hsgoc is not null) 
						   and ( trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n1=7) or loaitb_id in (89,90,146) )
						   and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 and nocuoc_n1 is null
						   
			   ;
  

-- nv,to,phong - ctvxhh		--theo doi khong co du lieu: T6
update ttkd_bsc.ct_bsc_ptm a 
   set thang_tlkpi=202407, thang_tlkpi_to=202407, thang_tlkpi_phong=202407
--		 select * from ttkd_bsc.ct_bsc_ptm a
		 where thang_ptm=202406 and thang_tlkpi_phong is null 
					   and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in (145,266)
					   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
					   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
					   and lydo_khongtinh_luong is null
					   and ( ( (ngay_luuhs_ttkd is not null or ngay_luuhs_ttvt is not null or ngay_scan is not null) and nop_du=1)  or mien_hsgoc is not null) 
					   and ( trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n1=7) or loaitb_id in (89,90,146) )
					   and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 and nocuoc_n1 is null
					   and ungdung_id=17
   ;
   
   
-- PGP: 	---theo doi du lieu khong co: T6
update ttkd_bsc.ct_bsc_ptm a
   set thang_tlkpi_hotro = 202407
--				 select * from ttkd_bsc.ct_bsc_ptm a
				 where thang_ptm=202406
								and thang_tldg_dt_nvhotro = 202407
								and thang_tlkpi_hotro is null
							    and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') 
							    and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
							    and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
							    and (lydo_khongtinh_luong is null or lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
							    and (trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n1=7) or loaitb_id in (89,90,146) )
							    and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 and nocuoc_n1 is null
							    and (nop_du=1 or mien_hsgoc is not null) 

    ;

   
-- Ky (n-2)
  -- to - dai ly
update ttkd_bsc.ct_bsc_ptm a
   set thang_tlkpi_to=202407
  -- 				select loai_ld from ttkd_bsc.ct_bsc_ptm a
				  where thang_ptm=202405
									and thang_tlkpi_to is null 
								  and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in (145,266)
								  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
								  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
								  and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%'
											    and (upper(ma_tiepthi_new) in ('DL_CNT','GTGT00001') or upper(nguoi_gt) in ('DL_CNT','GTGT00001') or upper(ma_nguoigt) in ('DL_CNT','GTGT00001'))))
								  and (trangthaitb_id_n2=1 or (loaitb_id=20 and trangthaitb_id_n2=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n2=7) or loaitb_id in (89,90,146) )
								  and ( ( (ngay_luuhs_ttkd is not null or ngay_luuhs_ttvt is not null or ngay_scan is not null) and nop_du=1)  or mien_hsgoc is not null) 
								  and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)>0 and nocuoc_n2 is null
								  
	  ;


-- phong - dai ly
update ttkd_bsc.ct_bsc_ptm a 
    set thang_tlkpi_phong=202407
 -- 		select * from ttkd_bsc.ct_bsc_ptm a
			 where thang_ptm=202405 and thang_tlkpi_phong is null 
						    and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in (145,266)
						    and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
						    and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
						    and (lydo_khongtinh_luong is null or lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
						    and ( ( (ngay_luuhs_ttkd is not null or ngay_luuhs_ttvt is not null or ngay_scan is not null) and nop_du=1)  or mien_hsgoc is not null) 
						    and (trangthaitb_id_n2=1 or (loaitb_id=20 and trangthaitb_id_n2=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n2=7) or loaitb_id in (89,90,146) )
						    and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)>0 and nocuoc_n2 is null
						    
    ;


-- nv.to,phong - kenh CTVXHH	--theo doi xoa
update ttkd_bsc.ct_bsc_ptm a 
    set thang_tlkpi = 202407, thang_tlkpi_to=202407, thang_tlkpi_phong=202407
--			  select * from ttkd_bsc.ct_bsc_ptm a
			 where thang_ptm=202405 and thang_tlkpi_phong is null 
					    and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in (145,266)
					    and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
					    and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
					    and lydo_khongtinh_luong is null
					    and ( ( (ngay_luuhs_ttkd is not null or ngay_luuhs_ttvt is not null or ngay_scan is not null) and nop_du=1)  or mien_hsgoc is not null) 
					    and (trangthaitb_id_n2=1 or (loaitb_id=20 and trangthaitb_id_n2=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n2=7) or loaitb_id in (89,90,146) )
					    and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)>0 and nocuoc_n2 is null
					    and ungdung_id=17
    ;
    

-- PGP
update ttkd_bsc.ct_bsc_ptm a
   set thang_tlkpi_hotro=202407
--			  select  thang_luong, thang_tldg_dt_nvhotro, thang_tlkpi_hotro from ttkd_bsc.ct_bsc_ptm a
			 where thang_ptm=202405 and chuquan_id in (145,266) 
							and thang_tldg_dt_nvhotro = 202407			----thang_tlkpi NVPTM co thi Hotro co
							and thang_tlkpi_hotro is null
						    and (loaitb_id not in (21,131) or ma_kh='GTGT rieng')
						    and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
						    and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
						    and (lydo_khongtinh_luong is null or lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
						    and (trangthaitb_id_n2=1 or (loaitb_id=20 and trangthaitb_id_n2=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n2=7) or loaitb_id in (89,90,146) )
						   and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0) +nvl(dthu_ps_n2,0)>0 and nocuoc_n2 is null
--						    and exists(select 1 from ttkd_bsc.nhanvien where thang = 202406 and  ma_pb='VNP0702600' and ma_nv=a.manv_hotro)
--			    and thang_luong = 11

    ;

commit;
rollback;
-- Ky n-3
   -- to - dai ly
update ttkd_bsc.ct_bsc_ptm a
   set thang_tlkpi_to = 202407
--			 select loai_ld, thang_tlkpi_to from  ttkd_bsc.ct_bsc_ptm a
			 where thang_ptm=202404 and thang_tlkpi_to is null 
						   and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in (145,266)
						   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
						   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
						   and (lydo_khongtinh_luong is null
									   or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%'
												and (upper(ma_tiepthi_new) in ('DL_CNT','GTGT00001') or upper(nguoi_gt) in ('DL_CNT','GTGT00001') or upper(ma_nguoigt) in ('DL_CNT','GTGT00001'))
											)
									)
						   and (trangthaitb_id_n3=1 or (loaitb_id=20 and trangthaitb_id_n3=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n3=7) or loaitb_id in (89,90,146) )
						   and ( ( (ngay_luuhs_ttkd is not null or ngay_luuhs_ttvt is not null or ngay_scan is not null) and nop_du=1)  or mien_hsgoc is not null) 
						   and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)>0 and nocuoc_n3 is null
						   
   ;
  
   
-- phong - daily
update ttkd_bsc.ct_bsc_ptm a 
   set thang_tlkpi_phong=202407
--				    select loai_ld from ttkd_bsc.ct_bsc_ptm a 
				 where thang_ptm=202404 and thang_tlkpi_phong is null 
							   and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in (145,266)
							   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
							   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
							   and (lydo_khongtinh_luong is null or lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
							   and ( ( (ngay_luuhs_ttkd is not null or ngay_luuhs_ttvt is not null or ngay_scan is not null) and nop_du=1)  or mien_hsgoc is not null) 
							   and (trangthaitb_id_n3=1 or (loaitb_id=20 and trangthaitb_id_n3=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n3=7) or loaitb_id in (89,90,146) )
							   and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)>0 and nocuoc_n3 is null
							   
   ;   


-- nv,to,phong - kenh CTVXHH
update ttkd_bsc.ct_bsc_ptm a 
   set thang_tlkpi=202407, thang_tlkpi_to=202407, thang_tlkpi_phong=202407 
--			    select * from ttkd_bsc.ct_bsc_ptm a 
			 where thang_ptm=202404 and thang_tlkpi_phong is null 
						   and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in (145,266)
						   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
						   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
						   and lydo_khongtinh_luong is null
						   and ( ( (ngay_luuhs_ttkd is not null or ngay_luuhs_ttvt is not null or ngay_scan is not null) and nop_du=1)  or mien_hsgoc is not null) 
						   and (trangthaitb_id_n3=1 or (loaitb_id=20 and trangthaitb_id_n3=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n3=7) or loaitb_id in (89,90,146) )
						   and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)>0 and nocuoc_n3 is null
						   and ungdung_id=17
			   ;

     
-- PGP 
update ttkd_bsc.ct_bsc_ptm a
   set thang_tlkpi_hotro = 202407
--			  select  thang_tldg_dt_nvhotro, thang_tlkpi_hotro from ttkd_bsc.ct_bsc_ptm a
			 where thang_ptm = 202404
							and thang_tlkpi_hotro is null
							and thang_tldg_dt_nvhotro is not null
						    and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') 
						    and (lydo_khongtinh_luong is null or lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
						    and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
						    and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
						    and (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n3=7) or loaitb_id in (89,90,146) )
						    and nocuoc_n3 is null    
						    
--						    and exists(select 1 from ttkd_bsc.nhanvien where thang = 202406 and ma_pb='VNP0702600' and ma_nv=a.manv_hotro)
    ;
    
commit;
		
-- Kiem tra
select * from ttkd_bsc.ct_bsc_ptm 
    where thang_ptm>=202402 and (loaitb_id<>21 or ma_kh='GTGT rieng')
        and thang_tlkpi_to is null and thang_tlkpi=202406;


select * from ttkd_bsc.ct_bsc_ptm 
    where thang_ptm>=202402 
        and (loaitb_id<>21 or ma_kh='GTGT rieng') and ma_to is not null 
        and (lydo_khongtinh_dongia is null
                    or (lydo_khongtinh_dongia not like '%Phat trien qua Dai ly%'
                            and lydo_khongtinh_dongia not like '%ptm kenh CTVXHH%'))
        and thang_tlkpi_to=202406 and thang_tldg_dt is null
        and (loai_ld not like '_LCN' or loai_ld is null)  ;

    
-- tinh kpi, ko tinh don gia
select thang_ptm, ten_pb, ten_to,dich_vu, ma_tb, trangthaitb_id_n3, nocuoc_n3, ngay_luuhs_ttkd,nop_du, sothang_dc
        ,loai_ld, thang_tldg_dt , lydo_khongtinh_dongia, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong
    from ttkd_bsc.ct_bsc_ptm 
    where thang_ptm>=202402
        and (loaitb_id<>21 or ma_kh='GTGT rieng') and lydo_khongtinh_dongia not like '%don gia%'
        and manv_ptm is not null 
        and ((thang_tldg_dt is null and thang_tlkpi=202406)
                or (thang_tldg_dt =202406 and thang_tlkpi is null)) ;   

             
select * from ttkd_bsc.ct_bsc_ptm
where chuquan_id in (145,266)
    and ( thang_tldg_dt=202406 or thang_tlkpi=202406 or thang_tlkpi_to=202406 or thang_tlkpi_phong=202406)
    and loaitb_id not in (20,21,149) and loaihd_id=1
    and (trangthai_tt_id is null or trangthai_tt_id=0);


select * from ttkd_bsc.ct_bsc_ptm_pgp
where (thang_tlkpi_hotro=202406 and thang_tldg_dt_nvhotro is null)
        or (thang_tldg_dt_nvhotro=202406 and thang_tlkpi_hotro is null );


select * from ttkd_bsc.ct_bsc_ptm_pgp
where lydo_khongtinh_dongia like '%Dai ly%' 
        and lydo_khongtinh_dongia not like '%No cuoc%'
        and lydo_khongtinh_dongia not like '%Trang thai%'
        and lydo_khongtinh_dongia not like '%doanh thu%'
        and manv_hotro is not null and thang_tldg_dt_nvhotro is null ;


select * from ttkd_bsc.ct_bsc_ptm a
    where chuquan_id in (145,266)
        and ( thang_tldg_dt=202406 or thang_tlkpi=202406 or thang_tlkpi_to=202406 or thang_tlkpi_phong=202406 or thang_tldg_dnhm=202406 or thang_tldg_dt_nvhotro = 202406)
        and loaitb_id not in (20,21,149) and loaihd_id=1
        and (trangthai_tt_id is null or trangthai_tt_id=0);
	   
select * from ttkd_bsc.ct_bsc_ptm a
    where chuquan_id in (145,266)
        and  ((thang_tldg_dt is null and thang_tldg_dt_nvhotro = 202406)
					--or (thang_tldg_dt = 202406 and thang_tldg_dt_nvhotro is null)
					or (thang_tldg_dt is null and THANG_TLKPI = 202406)
				)
	   ;


  

    
    
