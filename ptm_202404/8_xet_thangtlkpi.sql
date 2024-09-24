update ttkd_bsc.ct_bsc_ptm 
		    set thang_tlkpi=202404, thang_tlkpi_to=202404, thang_tlkpi_phong=202404
		    ---   select ma_tb, nguon, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, thang_tldg_dt from ttkd_bsc.ct_bsc_ptm  a
		    where (loaitb_id not in (21) or ma_kh='GTGT rieng') and thang_tlkpi is null and thang_tldg_dt=202404
		 --   and ma_tb = 'hcm_ca_00067906'
		 
		    ;


update ttkd_bsc.ct_bsc_ptm 
    set thang_tlkpi_hotro=202404
    
--     select ma_tb, nguon, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, thang_tldg_dt, thang_tlkpi_hotro from ttkd_bsc.ct_bsc_ptm  a
    where (loaitb_id not in (21) or ma_kh='GTGT rieng') and thang_tlkpi_hotro is null and thang_tldg_dt_nvhotro=202404
    ;
    commit;


update ttkd_bsc.ct_bsc_ptm a
   set thang_tlkpi_dnhm=202404, thang_tlkpi_dnhm_to=202404, thang_tlkpi_dnhm_phong=202404
   where (loaitb_id not in (21) or ma_kh='GTGT rieng') and thang_tldg_dnhm=202404;
   
   commit;
-- Ky n 
-- to: 
-- DL_CNT  - DT cua AM QLDL ko ghi nhan cho Truong line ngoai tru Dai ly thu tuc CNT
		update ttkd_bsc.ct_bsc_ptm a
		   set thang_tlkpi_to=202404
		 -- select chuquan_id, ma_tb, ma_nguoigt, ten_pb, ma_to, manv_ptm, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, lydo_khongtinh_luong,lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
		 where thang_ptm=202404 
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
			   set thang_tlkpi_dnhm_to=202404
			 -- select ma_tb, thang_tldg_dt, thang_tlkpi_dnhm, thang_tlkpi_dnhm_to, thang_tlkpi_dnhm_phong, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			  where thang_ptm=202404 and thang_tlkpi_dnhm_to is null and loaitb_id<>21
				  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
				  and  (upper(ma_tiepthi_new) in ('DL_CNT','GTGT00001') or upper(nguoi_gt) in ('DL_CNT','GTGT00001') or upper(ma_nguoigt) in ('DL_CNT','GTGT00001'))   
				  and  (tien_dnhm>0 or tien_sodep>0) 
				  and ((loaitb_id!=20 and trangthai_tt_id=1) or loaitb_id=20)
				  and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id );


   
  -- phong - Dai ly
		update ttkd_bsc.ct_bsc_ptm a 
		   set thang_tlkpi_phong=202404
		   -- select thang_ptm, ma_tb, ten_pb, ma_tiepthi, ma_tiepthi_new , ma_to, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, lydo_khongtinh_luong, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			where thang_ptm=202404 and thang_tlkpi_phong is null 
					   and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') and chuquan_id in (145,266)
					   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
					   and lydo_khongtinh_luong like '%Phat trien qua Dai ly%'
					   and (nop_du=1 or mien_hsgoc is not null) 
					   and (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id=7) or loaitb_id in (89,90,146) )
					   and dthu_ps is not null and nocuoc_ptm is null
					   and ((loaitb_id!=20 and trangthai_tt_id=1) or loaitb_id=20)
		   ;


-- nv, to, phong - kenh CTVXHH:
		update ttkd_bsc.ct_bsc_ptm a 
		   set thang_tlkpi=202404, thang_tlkpi_to=202404, thang_tlkpi_phong=202404
		    -- select thang_ptm, ungdung_id, ma_tb, ten_pb, ma_tiepthi, ma_tiepthi_new , ma_to, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, lydo_khongtinh_luong, lydo_khongtinh_dongia from ct_bsc_ptm a
			where thang_ptm=202404 and thang_tlkpi is null 
		   and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') and chuquan_id in (145,266)
		   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
		   and (nop_du=1 or mien_hsgoc is not null) 
		   and (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id=7) or loaitb_id in (89,90,146) )
		   and dthu_ps>0 and nocuoc_ptm is null
		   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149)) 
		   and ungdung_id=17
		   ;


-- dnhm - phong:  dai ly 
			update ttkd_bsc.ct_bsc_ptm a
			   set thang_tlkpi_dnhm_phong=202404
			 -- select ma_tb, thang_tlkpi_dnhm_phong, thang_tlkpi_dnhm, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			 where thang_ptm=202404 and thang_tlkpi_dnhm_phong is null
			   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
			   and lydo_khongtinh_luong like '%Phat trien qua Dai ly%' 
			   and (nop_du=1 or mien_hsgoc is not null) 
			   and (tien_dnhm>0 or tien_sodep>0) 
			   and dthu_ps is not null and nocuoc_ptm is null
			   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149)) 
			   and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
			   ;
 
 
-- dnhm - nv/to/phong - kenh CTVXHH
				update ttkd_bsc.ct_bsc_ptm a
				   set thang_tlkpi_dnhm=202404, thang_tlkpi_dnhm_to=202404, thang_tlkpi_dnhm_phong=202404
				 -- select ma_tb, thang_tlkpi_dnhm_phong, thang_tlkpi_dnhm, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
				 where thang_ptm=202404 and thang_tlkpi_dnhm_phong is null and thang_tlkpi=202404
				   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id) 
				   and (nop_du=1 or mien_hsgoc is not null) 
				   and (tien_dnhm>0 or tien_sodep>0) 
				   and dthu_ps is not null and nocuoc_ptm is null
				   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
				   and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
				   and ungdung_id=17
				   ;
 
 
-- Ky (n-1)  
  -- to: 
				update ttkd_bsc.ct_bsc_ptm a
				   set thang_tlkpi_to=202404
				 -- select ma_tb, dich_vu, lydo_khongtinh_luong, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
				 where thang_ptm=202403 and thang_tlkpi_to is null 
						   and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in (145,266)
						   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
						   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
						   and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%'
										and (upper(ma_tiepthi_new) in ('DL_CNT','GTGT00001') or upper(nguoi_gt) in ('DL_CNT','GTGT00001') or upper(ma_nguoigt) in ('DL_CNT','GTGT00001'))))
						   and ( trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n1=7) or loaitb_id in (89,90,146) )
						   and ( ( (ngay_luuhs_ttkd is not null or ngay_luuhs_ttvt is not null or ngay_scan is not null) and nop_du=1)  or mien_hsgoc is not null) 
						   and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 and nocuoc_n1 is null
				   ;


-- phong - dai ly
			update ttkd_bsc.ct_bsc_ptm a 
			   set thang_tlkpi_phong=202404
			 -- select * from ct_bsc_ptm a
			 where thang_ptm=202403 and thang_tlkpi_phong is null 
						   and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in (145,266)
						   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
						   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
						   and (lydo_khongtinh_luong is null or lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
						   and ( ( (ngay_luuhs_ttkd is not null or ngay_luuhs_ttvt is not null or ngay_scan is not null) and nop_du=1)  or mien_hsgoc is not null) 
						   and ( trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n1=7) or loaitb_id in (89,90,146) )
						   and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 and nocuoc_n1 is null
			   ;
  

-- nv,to,phong - ctvxhh
update ttkd_bsc.ct_bsc_ptm a 
   set thang_tlkpi=202404, thang_tlkpi_to=202404, thang_tlkpi_phong=202404
 -- select * from ct_bsc_ptm a
 where thang_ptm=202403 and thang_tlkpi_phong is null 
			   and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in (145,266)
			   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
			   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
			   and lydo_khongtinh_luong is null
			   and ( ( (ngay_luuhs_ttkd is not null or ngay_luuhs_ttvt is not null or ngay_scan is not null) and nop_du=1)  or mien_hsgoc is not null) 
			   and ( trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n1=7) or loaitb_id in (89,90,146) )
			   and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 and nocuoc_n1 is null
			   and ungdung_id=17
   ;
   
   
-- PGP: 
update ttkd_bsc.ct_bsc_ptm a
   set thang_tlkpi_hotro=202404
 -- select * from ct_bsc_ptm a
 where thang_ptm=202403 and thang_tlkpi_hotro is null
			    and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') 
			    and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
			    and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
			    and (lydo_khongtinh_luong is null or lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
			    and (trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n1=7) or loaitb_id in (89,90,146) )
			    and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 and nocuoc_n1 is null   
			    and exists(select 1 from ttkd_bsc.nhanvien_202404 where ma_pb='VNP0702600' and ma_nv=a.manv_hotro)
    ;

   
-- Ky (n-2)
  -- to - dai ly
update ttkd_bsc.ct_bsc_ptm a
   set thang_tlkpi_to=202404
  -- select * from ct_bsc_ptm a
  where thang_ptm=202402 and thang_tlkpi_to is null 
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
    set thang_tlkpi_phong=202404
 -- select * from ttkd_bsc.ct_bsc_ptm a
 where thang_ptm=202402 and thang_tlkpi_phong is null 
			    and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in (145,266)
			    and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
			    and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
			    and (lydo_khongtinh_luong is null or lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
			    and ( ( (ngay_luuhs_ttkd is not null or ngay_luuhs_ttvt is not null or ngay_scan is not null) and nop_du=1)  or mien_hsgoc is not null) 
			    and (trangthaitb_id_n2=1 or (loaitb_id=20 and trangthaitb_id_n2=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n2=7) or loaitb_id in (89,90,146) )
			    and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)>0 and nocuoc_n2 is null
    ;


-- nv.to,phong - kenh CTVXHH
update ttkd_bsc.ct_bsc_ptm a 
    set thang_tlkpi=202404, thang_tlkpi_to=202404, thang_tlkpi_phong=202404
 -- select * from ttkd_bsc.ct_bsc_ptm a
 where thang_ptm=202402 and thang_tlkpi_phong is null 
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
   set thang_tlkpi_hotro=202404
 -- select * from ttkd_bsc.ct_bsc_ptm a
 where thang_ptm=202402 and chuquan_id in (145,266) and thang_tlkpi_hotro is null
			    and (loaitb_id not in (21,131) or ma_kh='GTGT rieng')
			    and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
			    and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
			    and (lydo_khongtinh_luong is null or lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
			    and (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n2=7) or loaitb_id in (89,90,146) )
			    and nocuoc_n2 is null   
			    and exists(select 1 from ttkd_bsc.nhanvien_202404 where ma_pb='VNP0702600' and ma_nv=a.manv_hotro)
    ;


-- Ky n-3
   -- to - dai ly
update ttkd_bsc.ct_bsc_ptm a
   set thang_tlkpi_to=202404
-- select * from  ttkd_bsc.ct_bsc_ptm a
 where thang_ptm=202401 and thang_tlkpi_to is null 
			   and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in (145,266)
			   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
			   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
			   and (lydo_khongtinh_luong is null
					   or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%'
							and (upper(ma_tiepthi_new) in ('DL_CNT','GTGT00001') or upper(nguoi_gt) in ('DL_CNT','GTGT00001') or upper(ma_nguoigt) in ('DL_CNT','GTGT00001'))))
			   and (trangthaitb_id_n3=1 or (loaitb_id=20 and trangthaitb_id_n3=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n3=7) or loaitb_id in (89,90,146) )
			   and ( ( (ngay_luuhs_ttkd is not null or ngay_luuhs_ttvt is not null or ngay_scan is not null) and nop_du=1)  or mien_hsgoc is not null) 
			   and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)>0 and nocuoc_n3 is null
   ;
  
   
-- phong - daily
update ttkd_bsc.ct_bsc_ptm a 
   set thang_tlkpi_phong=202404
   -- select * from ttkd_bsc.ct_bsc_ptm a 
 where thang_ptm=202401 and thang_tlkpi_phong is null 
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
   set thang_tlkpi=202404, thang_tlkpi_to=202404, thang_tlkpi_phong=202404
   -- select * from ttkd_bsc.ct_bsc_ptm a 
 where thang_ptm=202401 and thang_tlkpi_phong is null 
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
   set thang_tlkpi_hotro=202404
 -- select * from ttkd_bsc.ct_bsc_ptm a
 where thang_ptm=202401 and thang_tlkpi_hotro is null
			    and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') 
			    and (lydo_khongtinh_luong is null or lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
			    and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
			    and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
			    and (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)) and trangthaitb_id_n3=7) or loaitb_id in (89,90,146) )
			    and nocuoc_n3 is null    
			    and exists(select 1 from ttkd_bsc.nhanvien_202404 where ma_pb='VNP0702600' and ma_nv=a.manv_hotro)
    ;
    
commit;
				-- 99 tb vnpts ptm 03/2024 co psc bat thuong
			update ttkd_bsc.ct_bsc_ptm a 
			    set --thang_tldg_dt=''
						THANG_TLDG_DNHM = null, THANG_TLKPI_DNHM = null, THANG_TLKPI_DNHM_TO = null, THANG_TLKPI_DNHM_PHONG = null
						, THANG_TLKPI = null, THANG_TLKPI_HOTRO = null, THANG_TLKPI_TO = null, THANG_TLKPI_PHONG = null
					  ,lydo_khongtinh_dongia='Mua goi 159V, dthu goi 145.454 nhung psc = 8.181, khuyen mai 136k, nhom TL d/n giai trinh'
		---   select * from ttkd_bsc.ct_bsc_ptm a 
			    where thang_ptm=202403  --thang 202403
				   and ma_tb in ('84838777901','84836777301','84838777832','84836777316','84836777623','84832777983','84839777806','84839777813','84833777902',
							    '84911777107','84916888745','84911888714','84911777821','84917666874','84912777592','84914777953','84914777261','84918333561',
							    '84914777063','84914666221','84914666281','84915999370','84914777153','84919555705','84914777961','84915777961','84916777132',
							    '84916777518','84915777397','84915777830','84916111602','84916111637','84917111301','84913666340','84829333635','84839333631',
							    '84917291769','84911798059','84838333622','84919888661','84918666927','84916668530','84916665821','84916668653','84916665912',
							    '84916669381','84916669308','84916669361','84916668953','84916668703','84916669053','84916668284','84916668063','84828885153',
							    '84816663053','84826662060','84916662757','84912226364','84826662061','84816663013','84916667219','84914999857','84833555623',
							    '84833555376','84914999850','84833555726','84914999854','84914999620','84914999853','84914999851','84833555132','84916333621',
							    '84918222976','84917999231','84911888310','84919998042','84919997046','84919994961','84919994738','84916664181','84913636112',
							    '84919991207','84919998524','84913636530','84916664162','84916664206','84916660748','84919996148','84916660642','84913838984',
							    '84856222070','84916660821','84919992264','84919993504','84856222304','84856222034','84823222041','84823222381','84823222143' ) 
						;

commit;
-- Kiem tra
select * from ttkd_bsc.ct_bsc_ptm 
    where thang_ptm>=202401 and (loaitb_id<>21 or ma_kh='GTGT rieng')
        and thang_tlkpi_to is null and thang_tlkpi=202404;


select * from ttkd_bsc.ct_bsc_ptm 
    where thang_ptm>=202401 
        and (loaitb_id<>21 or ma_kh='GTGT rieng') and ma_to is not null 
        and (lydo_khongtinh_dongia is null
                    or (lydo_khongtinh_dongia not like '%Phat trien qua Dai ly%'
                            and lydo_khongtinh_dongia not like '%ptm kenh CTVXHH%'))
        and thang_tlkpi_to=202404 and thang_tlkpi is null
        and (loai_ld not like '_LCN' or loai_ld is null)  ;

    
-- tinh kpi, ko tinh don gia
select thang_ptm, ten_pb, ten_to,dich_vu, ma_tb, trangthaitb_id_n3, nocuoc_n3, ngay_luuhs_ttkd,nop_du, sothang_dc
        ,loai_ld, thang_tldg_dt , lydo_khongtinh_dongia, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong
    from ttkd_bsc.ct_bsc_ptm 
    where thang_ptm>=202401 
        and (loaitb_id<>21 or ma_kh='GTGT rieng') and lydo_khongtinh_dongia not like '%don gia%'
        and manv_ptm is not null 
        and ((thang_tldg_dt is null and thang_tlkpi=202404)
                or (thang_tldg_dt =202404 and thang_tlkpi is null)) ;   

             
select * from ttkd_bsc.ct_bsc_ptm
where chuquan_id in (145,266)
    and ( thang_tldg_dt=202404 or thang_tlkpi=202404 or thang_tlkpi_to=202404 or thang_tlkpi_phong=202404)
    and loaitb_id not in (20,21,149) and loaihd_id=1
    and (trangthai_tt_id is null or trangthai_tt_id=0);


select * from ttkd_bsc.ct_bsc_ptm_pgp
where (thang_tlkpi_hotro=202404 and thang_tldg_dt_nvhotro is null)
        or (thang_tldg_dt_nvhotro=202404 and thang_tlkpi_hotro is null );


select * from ttkd_bsc.ct_bsc_ptm_pgp
where lydo_khongtinh_dongia like '%Dai ly%' 
        and lydo_khongtinh_dongia not like '%No cuoc%'
        and lydo_khongtinh_dongia not like '%Trang thai%'
        and lydo_khongtinh_dongia not like '%doanh thu%'
        and manv_hotro is not null and thang_tldg_dt_nvhotro is null ;


select * from ttkd_bsc.ct_bsc_ptm a
    where chuquan_id in (145,266)
        and ( thang_tldg_dt=202404 or thang_tlkpi=202404 or thang_tlkpi_to=202404 or thang_tlkpi_phong=202404 or thang_tldg_dnhm=202404)
        and loaitb_id not in (20,21,149) and loaihd_id=1
        and (trangthai_tt_id is null or trangthai_tt_id=0);
 

    
    
