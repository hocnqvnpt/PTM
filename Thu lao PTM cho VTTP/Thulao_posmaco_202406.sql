-- Kiem tra
select distinct lydo_khongtinh_dongia 
from ttkd_bsc.ct_bsc_ptm
where nhom_tiepthi = 2 and thang_ptm>=202310  
                and ungdung_id = 17 and thang_tldg_dt is null;
    
    
-- Chi tiet trong ky:
	---chi tinh 5 loai hinh: VNPtt, VNPts, Fiber, MyTV, Mega
	---thang_tldg_dt : thang duoc tinh luong don gia (null: chua tinh, 999999 khong dc tinh, yyyymm thang dc tinh)
	---thang_ptm: thang insert table
	---nhom_tiepthi: tinh nhan vien VTTP (danh muc ttkd_bsc.dm_nhomld key nhomld_id)
	---thoi gian tinh trong vong 4 thang tinh tu thang_ptm, rieng VNPtt tinh 1 thang
	---VTTP chi tinh PTM, TTKD tinh PTM, nang toc do, tai lap
drop table ttkd_bsc.ct_bsc_ptm_potmasco_202406;

select distinct dich_vu from ttkd_bsc.ct_bsc_ptm_potmasco_202406;
select distinct TENKIEU_LD from ttkd_bsc.ct_bsc_ptm_potmasco_202406;
            
		  create table ttkd_bsc.ct_bsc_ptm_potmasco_202406 as
                select * from ttkd_bsc.ct_bsc_ptm
                where 
							(manv_ptm in (select ma_nv from ttkd_bsc.nhanvien_vttp_potmasco where thang = 202406)
								or manv_hotro in (select ma_nv from ttkd_bsc.nhanvien_vttp_potmasco where thang = 202406)
							)
                                and ((thang_ptm between 202403 and 202405     ---thang n-3
										  and ((loaihd_id=1 and loaitb_id in (11,58,61)) or loaitb_id=20)
										  and (thang_tldg_dt = 202406 or thang_tldg_dt is null))          ---thang n
                                            )                       ---thang n
    ;
    

                    
-- Tao bangluong_dongia_202402_vttp
drop table ttkd_bsc.bangluong_dongia_potmasco;
ALTER TABLE ttkd_bsc.bangluong_dongia_potmasco RENAME column nv_thang to thang;
create table ttkd_bsc.bangluong_dongia_potmasco (
                    ma_nv               varchar(10),
                    ten_nv               varchar(50),
                    ma_vtcv            varchar(20),
                    ten_vtcv            varchar(100),
                    ma_to               varchar(10),
                    ten_to               varchar(100),
                    ma_pb         varchar(10),
                    ten_pb         varchar(100),
                    
                    sl_brcd_chitl number,
                    sl_mytv_chitl number,
                    sl_vnpts_chitl number,
                    sl_vnptt_chitl number,
                    tong_sl_chitl number,
                    
                    dt_brcd_chitl number,
                    dt_mytv_chitl number,
                    dt_vnpts_chitl number,
                    dt_vnptt_chitl number,
                    tong_dt_chitl number,
                    
                    dt_brcd_chuachitl number,
                    dt_mytv_chuachitl number,
                    dt_vnpts_chuachitl number,
                    dt_vnptt_chuachitl number,
                    tong_dt_chuachitl number,
                    
                    sl_brcd_chuachitl number,
                    sl_mytv_chuachitl number,
                    sl_vnpts_chuachitl number,
                    sl_vnptt_chuachitl number,
                    tong_sl_chuachitl number,
                    
                    tl_brcd number,
                    tl_mytv number,
                    tl_vnpts number,
                    tl_vnptt number,
                    tong_tl number ,
                    
                    tl_thuhoi_brcd number,
                    tl_thuhoi_mytv number,
                    tl_thuhoi_vnpts number,
                    tl_thuhoi_vnptt number,
                    tong_tl_thuhoi number,
                    
                    sl_thuhoi_brcd number,
                    sl_thuhoi_mytv number,
                    sl_thuhoi_vnpts number,
                    sl_thuhoi_vnptt number,
                    tong_sl_thuhoi number,
                    
                    thang number 
                    );
create index ttkd_bsc.bangluong_dongia_potmasco_manv on ttkd_bsc.bangluong_dongia_potmasco (ma_nv);

select * from ttkd_bsc.nhanvien_vttp_potmasco where thang = 202406;
select * from  ttkd_bsc.bangluong_dongia_potmasco where thang = 202406;

insert into ttkd_bsc.bangluong_dongia_potmasco
      (ma_nv,ten_nv,ma_vtcv,ten_vtcv,ma_pb,ten_pb,ma_to,ten_to, thang )
		SELECT ma_nv,ten_nv,ma_vtcv,ten_vtcv,ma_pb,ten_pb,ma_to,ten_to, thang           --thang n
		  FROM ttkd_bsc.nhanvien_vttp_potmasco a
		  where thang = 202406
--					exists(select 1 from ttkd_bsc.ct_bsc_ptm_202406_vttp where manv_ptm=a.ma_nv)
--						and thang=(select max(thang) from ttkd_bsc.nhanvien_vttp where ma_nv=a.ma_nv)
	   ;
        
        select * from ttkd_bsc.nhanvien_vttp_potmasco;
	   select * from ttkd_bsc.x_th_tlptm_potmasco;
		commit;
    
-- tong hop:  
		drop table ttkd_bsc.x_th_tlptm_potmasco
		;
					create table ttkd_bsc.x_th_tlptm_potmasco as
					select manv_ptm ma_nv, 
							  (select ten_pb from ttkd_bsc.nhanvien_vttp_potmasco where thang = 202406 and ma_nv = a.manv_ptm) ten_donvi,            
							  sum(nvl(sl_brcd_chitl,0)) sl_brcd_chitl, 
							  sum(nvl(sl_mytv_chitl,0)) sl_mytv_chitl,
							  sum(nvl(sl_vnpts_chitl,0)) sl_vnpts_chitl,
							  sum(nvl(sl_vnptt_chitl,0)) sl_vnptt_chitl,            
							  
							  sum(nvl(dt_brcd_chitl,0)) dt_brcd_chitl, 
							  sum(nvl(dt_mytv_chitl,0)) dt_mytv_chitl,
							  sum(nvl(dt_vnpts_chitl,0)) dt_vnpts_chitl,
							  sum(nvl(dt_vnptt_chitl,0)) dt_vnptt_chitl,
					
							  sum(nvl(tl_brcd,0)) tl_brcd, 
							  sum(nvl(tl_mytv,0)) tl_mytv,
							  sum(nvl(tl_vnpts,0)) tl_vnpts,
							  sum(nvl(tl_vnptt,0)) tl_vnptt
							  
					from
					(
					    -- dthu + thu lao duoc tinh cdbr, mytv, vnpts
					    select  manv_ptm, 
							  count(case when loaitb_id in (11,58) then 1 end) sl_brcd_chitl,
							  count(case when loaitb_id=61 then 1 end) sl_mytv_chitl,
							  count(case when loaitb_id=20  then 1 end) sl_vnpts_chitl,
							  0 as sl_vnptt_chitl,
							  
							  sum(case when loaitb_id in (11,58) then doanhthu_dongia_nvptm end) dt_brcd_chitl,
							  sum(case when loaitb_id=61 then doanhthu_dongia_nvptm end) dt_mytv_chitl,
							  sum(case when loaitb_id=20  then doanhthu_dongia_nvptm end) dt_vnpts_chitl,
							  0 as dt_vnptt_chitl,
					
							  sum(case when loaitb_id in (11,58) then luong_dongia_nvptm end) tl_brcd,
							  sum(case when loaitb_id=61 then luong_dongia_nvptm end) tl_mytv,
							  sum(case when loaitb_id=20  then luong_dongia_nvptm end) tl_vnpts,
							  0 as tl_vnptt
					
					    from ttkd_bsc.ct_bsc_ptm_potmasco_202406
					    where thang_tldg_dt = 202406 and loaitb_id != 21          ---thang n---
										and manv_ptm is not null
					    group by manv_ptm 
					    
					    union all
								 select  manv_hotro, 
									  count(case when loaitb_id in (11,58) then 1 end) sl_brcd_chitl,
									  count(case when loaitb_id=61 then 1 end) sl_mytv_chitl,
									  count(case when loaitb_id=20  then 1 end) sl_vnpts_chitl,
									  0 as sl_vnptt_chitl,
									  
									  sum(case when loaitb_id in (11,58) then doanhthu_dongia_nvhotro end) dt_brcd_chitl,
									  sum(case when loaitb_id=61 then doanhthu_dongia_nvhotro end) dt_mytv_chitl,
									  sum(case when loaitb_id=20  then doanhthu_dongia_nvhotro end) dt_vnpts_chitl,
									  0 as dt_vnptt_chitl,
							
									  sum(case when loaitb_id in (11,58) then luong_dongia_nvhotro end) tl_brcd,
									  sum(case when loaitb_id=61 then luong_dongia_nvhotro end) tl_mytv,
									  sum(case when loaitb_id=20  then luong_dongia_nvhotro end) tl_vnpts,
									  0 as tl_vnptt
							
							    from ttkd_bsc.ct_bsc_ptm_potmasco_202406
							    where thang_tldg_dt_nvhotro = 202406 and loaitb_id != 21          ---thang n---
												and manv_hotro is not null
							    group by manv_hotro 
					   
					   union all 
					    -- dthu goi vnptt 
					    select  manv_ptm, 
							  0 as sl_brcd_chitl,
							  0 as sl_mytv_chitl,
							  0 as sl_vnpts_chitl,
							  0 as sl_vnptt_chitl,
							  
							  0 as dt_brcd_chitl,
							  0 as dt_mytv_chitl,
							  0 as dt_vnpts_chitl,
							  sum(dthu_goi_goc) dt_vnptt_chitl,        -- doanhthu_kpi_nvhotro da bao gom dthu dnhm + dthu goi
					
							  0 as tl_brcd,
							  0 as tl_mytv,
							  0 as tl_vnpts,          
							  sum(luong_dongia_nvptm) tl_vnptt
					    -- select *  
					    from ttkd_bsc.ct_bsc_ptm_potmasco_202406
					    where loaitb_id = 21 and thang_ptm = 202406 and thang_tldg_dt = 202406              --thang n
					    group by manv_ptm  
					
					   
					    union all
						   -- dnhm brcd, mytv, vnpts, vnptt duoc tinh
					    select  manv_ptm,
							  0 as sl_brcd_chitl,
							  0 as sl_mytv_chitl,
							  0 as sl_vnpts_chitl,
							  count(case when loaitb_id=21  then 1 end)  sl_vnptt_chitl,
							  
							  sum(case when loaitb_id in (11,58) then doanhthu_dongia_dnhm end) dt_brcd_chitl,
							  sum(case when loaitb_id=61 then doanhthu_dongia_dnhm end) dt_mytv_chitl,
							  sum(case when loaitb_id=20  then doanhthu_dongia_dnhm end) dt_vnpts_chitl,
							  sum(case when loaitb_id=21 then tien_dnhm end ) dt_vnptt_chitl,
					
							  sum(case when loaitb_id in (11,58) then luong_dongia_dnhm_nvptm end) tl_brcd,
							  sum(case when loaitb_id=61 then luong_dongia_dnhm_nvptm end) tl_mytv,
							  sum(case when loaitb_id=20 then luong_dongia_dnhm_nvptm end) tl_vnpts,
							  sum(case when loaitb_id=21 then luong_dongia_dnhm_nvptm end)  as tl_vnptt
					    -- select *
					    from ttkd_bsc.ct_bsc_ptm_potmasco_202406
					    where thang_tldg_dnhm = 202406      ----thang n
									and manv_ptm is not null
					    group by manv_ptm 
					) a
					where manv_ptm is not null
					group by manv_ptm 
;


-- sltb + dthu goi chua duoc tinh
		drop table ttkd_bsc.x_th_tlptm_potmasco_chuachitl
;
					create table ttkd_bsc.x_th_tlptm_potmasco_chuachitl as
					select manv_ptm ma_nv, 
							  (select ten_pb from ttkd_bsc.nhanvien_vttp_potmasco where thang = 202406 and ma_nv=a.manv_ptm) ten_donvi,
							  sum(sl_brcd_chuachitl) sl_brcd_chuachitl, 
							  sum(sl_mytv_chuachitl) sl_mytv_chuachitl,
							  sum(sl_vnpts_chuachitl) sl_vnpts_chuachitl,
							  sum(sl_vnptt_chuachitl) sl_vnptt_chuachitl,
							  
							  sum(nvl(dt_brcd_chuachitl,0)) dt_brcd_chuachitl, 
							  sum(nvl(dt_mytv_chuachitl,0)) dt_mytv_chuachitl,
							  sum(nvl(dt_vnpts_chuachitl,0)) dt_vnpts_chuachitl,
							  sum(nvl(dt_vnptt_chuachitl,0)) dt_vnptt_chuachitl
					from
					    (select  manv_ptm, 
							  count(case when loaitb_id in (11,58) then 1 end) sl_brcd_chuachitl,
							  count(case when loaitb_id=61 then 1 end) sl_mytv_chuachitl,
							  count(case when loaitb_id=20  then 1 end) sl_vnpts_chuachitl,
							  0 as  sl_vnptt_chuachitl,
							  
							  sum(case when loaitb_id in (11,58) then doanhthu_dongia_nvptm end) dt_brcd_chuachitl,
							  sum(case when loaitb_id=61 then doanhthu_dongia_nvptm end) dt_mytv_chuachitl,
							  sum(case when loaitb_id=20  then doanhthu_dongia_nvptm end) dt_vnpts_chuachitl,
							  0 as  dt_vnptt_chuachitl
							  
					    from ttkd_bsc.ct_bsc_ptm_potmasco_202406
					    where loaitb_id in (11,58,61,20) and thang_tldg_dt is null  and manv_ptm is not null
					    group by manv_ptm 
					    
					    union all
								select  manv_hotro, 
										  count(case when loaitb_id in (11,58) then 1 end) sl_brcd_chuachitl,
										  count(case when loaitb_id=61 then 1 end) sl_mytv_chuachitl,
										  count(case when loaitb_id=20  then 1 end) sl_vnpts_chuachitl,
										  0 as  sl_vnptt_chuachitl,
										  
										  sum(case when loaitb_id in (11,58) then doanhthu_dongia_nvhotro end) dt_brcd_chuachitl,
										  sum(case when loaitb_id=61 then doanhthu_dongia_nvhotro end) dt_mytv_chuachitl,
										  sum(case when loaitb_id=20  then doanhthu_dongia_nvhotro end) dt_vnpts_chuachitl,
										  0 as  dt_vnptt_chuachitl
										  
								    from ttkd_bsc.ct_bsc_ptm_potmasco_202406
								    where loaitb_id in (11,58,61,20) and thang_tldg_dt_nvhotro is null and manv_hotro is not null
								    group by manv_hotro 
					    union all
					    select  manv_ptm, 
							  0 as sl_brcd_chuachitl,
							  0 as sl_mytv_chuachitl,
							  0 as sl_vnpts_chuachitl,
							  count(*) as sl_vnptt_chuachitl,
							  
							  0 as dt_brcd_chuachitl,
							  0 as dt_mytv_chuachitl,
							  0 as dt_vnpts_chuachitl,
							  sum(tien_dnhm) dt_vnptt_chuachitl
					    from ttkd_bsc.ct_bsc_ptm_potmasco_202406
					    where loaitb_id=21 and thang_tldg_dnhm is null -- and nvl(doanhthu_kpi_nvhotro,0)>0
					    group by manv_ptm
					    ) a
					group by manv_ptm
		;
    
 
-- ptm cdbr + gtgt +vnpts             
			update ttkd_bsc.bangluong_dongia_potmasco a
				 set 
					  sl_brcd_chitl='',
					  sl_mytv_chitl='',
					  sl_vnpts_chitl='',
					  sl_vnptt_chitl='',
					  tong_sl_chitl='',
					  
					  dt_brcd_chitl='',
					  dt_mytv_chitl='',
					  dt_vnpts_chitl='',
					  dt_vnptt_chitl='',
					  tong_dt_chitl='',
			
					  tl_brcd='',
					  tl_mytv='',
					  tl_vnpts='',
					  tl_vnptt='',
					  tong_tl='',
					  
					  sl_brcd_chuachitl='',
					  sl_mytv_chuachitl='',
					  sl_vnpts_chuachitl='',
					  sl_vnptt_chuachitl='',
					  tong_sl_chuachitl='',
			
					  dt_brcd_chuachitl='',
					  dt_mytv_chuachitl='',
					  dt_vnpts_chuachitl='',
					  dt_vnptt_chuachitl='',
					  tong_dt_chuachitl=''
				where thang = 202406
	;
    
            
update ttkd_bsc.bangluong_dongia_potmasco a
            set ( sl_brcd_chitl, sl_mytv_chitl, sl_vnpts_chitl, sl_vnptt_chitl, 
                    dt_brcd_chitl, dt_mytv_chitl, dt_vnpts_chitl, dt_vnptt_chitl,
                    tl_brcd, tl_mytv, tl_vnpts, tl_vnptt)
           =
		 (select sl_brcd_chitl, sl_mytv_chitl, sl_vnpts_chitl, sl_vnptt_chitl, 
                    dt_brcd_chitl, dt_mytv_chitl, dt_vnpts_chitl, dt_vnptt_chitl,
                    tl_brcd, tl_mytv, tl_vnpts, tl_vnptt
             from ttkd_bsc.x_th_tlptm_potmasco
             where ma_nv = a.ma_nv)
--     select * from ttkd_bsc.bangluong_dongia_potmasco a
    where thang = 202406
				and exists(select 1 from ttkd_bsc.x_th_tlptm_potmasco where ma_nv=a.ma_nv)
    
    
    ;


update ttkd_bsc.bangluong_dongia_potmasco a
            set (sl_brcd_chuachitl, sl_mytv_chuachitl, sl_vnpts_chuachitl, sl_vnptt_chuachitl,
                    dt_brcd_chuachitl, dt_mytv_chuachitl, dt_vnpts_chuachitl, dt_vnptt_chuachitl)
            =(select sl_brcd_chuachitl, sl_mytv_chuachitl, sl_vnpts_chuachitl, sl_vnptt_chuachitl,
                    dt_brcd_chuachitl, dt_mytv_chuachitl, dt_vnpts_chuachitl, dt_vnptt_chuachitl
              from ttkd_bsc.x_th_tlptm_potmasco_chuachitl
              where ma_nv=a.ma_nv)
    -- select * from ttkd_bsc.bangluong_dongia_potmasco a
    where thang = 202406
				and exists(select 1 from ttkd_bsc.x_th_tlptm_potmasco_chuachitl where ma_nv=a.ma_nv)
    
    ;
    
    

update ttkd_bsc.bangluong_dongia_potmasco set tong_dt_chitl='', tong_tl='' , tong_dt_chuachitl='',  tong_sl_chitl='', tong_sl_chuachitl='' where thang = 202406
;
			update ttkd_bsc.bangluong_dongia_potmasco a
					  set  tong_dt_chitl      = nvl(dt_brcd_chitl,0)+nvl(dt_mytv_chitl,0)+nvl(dt_vnpts_chitl,0)+nvl(dt_vnptt_chitl,0)    
							,tong_tl                = nvl(tl_brcd,0)+nvl(tl_mytv,0)+nvl(tl_vnpts,0)+nvl(tl_vnptt,0)
							,tong_dt_chuachitl = nvl(dt_brcd_chuachitl,0)+nvl(dt_mytv_chuachitl,0)+nvl(dt_vnpts_chuachitl,0)+nvl(dt_vnptt_chuachitl,0)
							,tong_sl_chitl      = nvl(sl_brcd_chitl,0)+nvl(sl_mytv_chitl,0)+nvl(sl_vnpts_chitl,0)+nvl(sl_vnptt_chitl,0)   
							,tong_sl_chuachitl = nvl(sl_brcd_chuachitl,0)+nvl(sl_mytv_chuachitl,0)+nvl(sl_vnpts_chuachitl,0)+nvl(sl_vnptt_chuachitl,0)
				where thang = 202406
				;


update ttkd_bsc.bangluong_dongia_potmasco set dt_brcd_chitl='' where dt_brcd_chitl=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set dt_mytv_chitl='' where dt_mytv_chitl=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set dt_vnpts_chitl='' where dt_vnpts_chitl=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set dt_vnptt_chitl='' where dt_vnptt_chitl=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set tong_dt_chitl='' where tong_dt_chitl=0 and thang = 202406;

update ttkd_bsc.bangluong_dongia_potmasco set tl_brcd='' where tl_brcd=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set tl_mytv='' where tl_mytv=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set tl_vnpts='' where tl_vnpts=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set tl_vnptt='' where tl_vnptt=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set tong_tl='' where tong_tl=0 and thang = 202406;
            
update ttkd_bsc.bangluong_dongia_potmasco set dt_brcd_chuachitl='' where dt_brcd_chuachitl=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set dt_mytv_chuachitl='' where dt_mytv_chuachitl=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set dt_vnpts_chuachitl='' where dt_vnpts_chuachitl=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set dt_vnptt_chuachitl='' where dt_vnptt_chuachitl=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set tong_dt_chuachitl='' where tong_dt_chuachitl=0 and thang = 202406;
            
update ttkd_bsc.bangluong_dongia_potmasco set sl_brcd_chitl='' where sl_brcd_chitl=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set sl_mytv_chitl='' where sl_mytv_chitl=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set sl_vnpts_chitl='' where sl_vnpts_chitl=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set sl_vnptt_chitl='' where sl_vnptt_chitl=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set tong_sl_chitl='' where tong_sl_chitl=0 and thang = 202406;

update ttkd_bsc.bangluong_dongia_potmasco set sl_brcd_chuachitl='' where sl_brcd_chuachitl=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set sl_mytv_chuachitl='' where sl_mytv_chuachitl=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set sl_vnpts_chuachitl='' where sl_vnpts_chuachitl=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set sl_vnptt_chuachitl='' where sl_vnptt_chuachitl=0 and thang = 202406;
update ttkd_bsc.bangluong_dongia_potmasco set tong_sl_chuachitl='' where tong_sl_chuachitl=0 and thang = 202406;

            commit;
-- Thu hoi:
				update ttkd_bsc.bangluong_dongia_potmasco a  
				   set tl_thuhoi_brcd = '', tl_thuhoi_mytv = '', tl_thuhoi_vnpts = '', tl_thuhoi_vnptt = '', tong_tl_thuhoi = ''
						, sl_thuhoi_brcd='', sl_thuhoi_mytv='', sl_thuhoi_vnpts='', sl_thuhoi_vnptt='', tong_sl_thuhoi=''
				   where tong_tl_thuhoi is not null
								and thang = 202406
				   ;
--   select * from ttkd_bsc.bangluong_dongia_202406_vttp a
				update ttkd_bsc.bangluong_dongia_potmasco a  
				   set (tl_thuhoi_brcd, tl_thuhoi_mytv, tl_thuhoi_vnpts, tl_thuhoi_vnptt, tong_tl_thuhoi, sl_thuhoi_brcd, sl_thuhoi_mytv, sl_thuhoi_vnpts, sl_thuhoi_vnptt, tong_sl_thuhoi) 
							 = (select sum(case when loaitb_id =58 then tien_thuhoi end)
										  , sum(case when loaitb_id =61 then tien_thuhoi end)
										  , sum(case when loaitb_id =20 then tien_thuhoi end)
										  , sum(case when loaitb_id =21 then tien_thuhoi end)
										  , sum(case when loaitb_id in (58, 61, 20, 21) then tien_thuhoi end)
										  , sum(case when loaitb_id =58 then 1 end)
										  , sum(case when loaitb_id =61 then 1 end)
										  , sum(case when loaitb_id =20 then 1 end)
										  , sum(case when loaitb_id =21 then 1 end)
										  ,count(case when loaitb_id in (58, 61, 20, 21) then 1 end)
								  from ttkd_bsc.ct_thuhoi  
								  where thang = 202406 and ma_nv = a.ma_nv)             ---thang n
--				  select * from ttkd_bsc.bangluong_dongia_potmasco a  
				   where exists(select 1 from ttkd_bsc.ct_thuhoi 
									   where thang = 202406 and ma_nv = a.ma_nv)                         ---thang n
				;
				select * from ttkd_bsc.ct_thuhoi  
								  where thang=202406;
				
			delete from ttkd_bsc.bangluong_dongia_potmasco
			    where tong_tl is null and tong_dt_chuachitl is null and tong_sl_chitl is null and tong_sl_chuachitl is null and tong_sl_thuhoi is null;
    
    
			select *	from ttkd_bsc.bangluong_dongia_potmasco
				where thang = 202406;
commit;


-- BBXN-DTPTM-THULAO - Tong hop doanh thu/thu lao PTM theo TTVT:
			select rownum stt, ten_pb, dt_brcd_chitl, dt_mytv_chitl, dt_vnpts_chitl, dt_vnptt_chitl, tong_dt_chitl
							, dt_brcd_chuachitl, dt_mytv_chuachitl, dt_vnpts_chuachitl, dt_vnptt_chuachitl, tong_dt_chuachitl
							, tl_brcd, tl_mytv, tl_vnpts, tl_vnptt, tong_tl
						  , tl_thuhoi_brcd, tl_thuhoi_mytv, tl_thuhoi_vnpts, tl_thuhoi_vnptt, tong_tl_thuhoi 
		from (
					select ten_pb,
							 sum(dt_brcd_chitl) dt_brcd_chitl,
							 sum(dt_mytv_chitl) dt_mytv_chitl,
							 sum(dt_vnpts_chitl) dt_vnpts_chitl,
							 sum(dt_vnptt_chitl) dt_vnptt_chitl,
							 sum(tong_dt_chitl) tong_dt_chitl,
						
							 sum(dt_brcd_chuachitl) dt_brcd_chuachitl,
							 sum(dt_mytv_chuachitl) dt_mytv_chuachitl,
							 sum(dt_vnpts_chuachitl) dt_vnpts_chuachitl,
							 sum(dt_vnptt_chuachitl) dt_vnptt_chuachitl,
							 sum(tong_dt_chuachitl) tong_dt_chuachitl,
							 
							 sum(tl_brcd) tl_brcd,
							 sum(tl_mytv) tl_mytv,
							 sum(tl_vnpts) tl_vnpts,
							 sum(tl_vnptt) tl_vnptt,
							 sum(tong_tl) tong_tl,
							 
							 sum(tl_thuhoi_brcd) tl_thuhoi_brcd,
							 sum(tl_thuhoi_mytv) tl_thuhoi_mytv,
							 sum(tl_thuhoi_vnpts) tl_thuhoi_vnpts,
							 sum(tl_thuhoi_vnptt) tl_thuhoi_vnptt,
							 sum(tong_tl_thuhoi) tong_tl_thuhoi
					from ttkd_bsc.bangluong_dongia_potmasco
						where thang = 202406
										and (tong_tl is not null or tong_dt_chuachitl is not null or tong_tl_thuhoi is not null)
						group by ten_pb
						order by 1
			)
		union all
				select null, 'Tong cong',
					 sum(dt_brcd_chitl) dt_brcd_chitl,
					 sum(dt_mytv_chitl) dt_mytv_chitl,
					 sum(dt_vnpts_chitl) dt_vnpts_chitl,
					 sum(dt_vnptt_chitl) dt_vnptt_chitl,
					 sum(tong_dt_chitl) tong_dt_chitl,
				
					 sum(dt_brcd_chuachitl) dt_brcd_chuachitl,
					 sum(dt_mytv_chuachitl) dt_mytv_chuachitl,
					 sum(dt_vnpts_chuachitl) dt_vnpts_chuachitl,
					 sum(dt_vnptt_chuachitl) dt_vnptt_chuachitl,
					 sum(tong_dt_chuachitl) tong_dt_chuachitl,
					 
					 sum(tl_brcd) tl_brcd,
					 sum(tl_mytv) tl_mytv,
					 sum(tl_vnpts) tl_vnpts,
					 sum(tl_vnptt) tl_vnptt,
					 sum(tong_tl) tong_tl,
					 
					 sum(tl_thuhoi_brcd) tl_thuhoi_brcd,
					 sum(tl_thuhoi_mytv) tl_thuhoi_mytv,
					 sum(tl_thuhoi_vnpts) tl_thuhoi_vnpts,
					 sum(tl_thuhoi_vnptt) tl_thuhoi_vnptt,
					 sum(tong_tl_thuhoi) tong_tl_thuhoi
				from ttkd_bsc.bangluong_dongia_potmasco
				where thang = 202406
								and (tong_tl is not null or tong_dt_chuachitl is not null or tong_tl_thuhoi is not null)
;

select tl_thuhoi_brcd, tl_thuhoi_mytv, tl_thuhoi_vnpts, tl_thuhoi_vnptt, tong_tl_thuhoi
--			sum(tl_thuhoi_brcd) tl_thuhoi_brcd,
--			 sum(tl_thuhoi_mytv) tl_thuhoi_mytv,
--			 sum(tl_thuhoi_vnpts) tl_thuhoi_vnpts,
--			 sum(tl_thuhoi_vnptt) tl_thuhoi_vnptt,
--			 sum(tong_tl_thuhoi) tong_tl_thuhoi
from ttkd_bsc.bangluong_dongia_202406_vttp
where (tong_tl is not null or tong_dt_chuachitl is not null or tong_tl_thuhoi is not null) and ma_pb = 'HCM160000';
order by 1
;

-- BBXN-TBPTM - Tong hop sltb PTM theo TTVT:
			select rownum stt, ten_pb, sl_brcd_chitl, sl_mytv_chitl, sl_vnpts_chitl, sl_vnptt_chitl, tong_sl_chitl, sl_brcd_chuachitl, sl_mytv_chuachitl,
					  sl_vnpts_chuachitl, sl_vnptt_chuachitl, tong_sl_chuachitl, sl_thuhoi_brcd, sl_thuhoi_mytv, sl_thuhoi_vnpts, sl_thuhoi_vnptt, tong_sl_thuhoi
			from (
						select ten_pb,
							 sum(sl_brcd_chitl) sl_brcd_chitl,
							 sum(sl_mytv_chitl) sl_mytv_chitl,
							 sum(sl_vnpts_chitl) sl_vnpts_chitl,
							 sum(sl_vnptt_chitl) sl_vnptt_chitl,
							 sum(tong_sl_chitl) tong_sl_chitl,
						
							 sum(sl_brcd_chuachitl) sl_brcd_chuachitl,
							 sum(sl_mytv_chuachitl) sl_mytv_chuachitl,
							 sum(sl_vnpts_chuachitl) sl_vnpts_chuachitl,
							 sum(sl_vnptt_chuachitl) sl_vnptt_chuachitl,
							 sum(tong_sl_chuachitl) tong_sl_chuachitl,
							 
							 sum(sl_thuhoi_brcd) sl_thuhoi_brcd,
							 sum(sl_thuhoi_mytv) sl_thuhoi_mytv,
							 sum(sl_thuhoi_vnpts) sl_thuhoi_vnpts,
							 sum(sl_thuhoi_vnptt) sl_thuhoi_vnptt,
							 sum(tong_sl_thuhoi) tong_sl_thuhoi
						from ttkd_bsc.bangluong_dongia_potmasco
						where thang = 202406
										and (tong_tl is not null or tong_dt_chuachitl is not null or tong_tl_thuhoi is not null)
						group by ma_pb, ten_pb
						order by 1
			)
			union all
			select null, 'Tong cong',   
				 sum(sl_brcd_chitl) sl_brcd_chitl,
				 sum(sl_mytv_chitl) sl_mytv_chitl,
				 sum(sl_vnpts_chitl) sl_vnpts_chitl,
				 sum(sl_vnptt_chitl) sl_vnptt_chitl,
				 sum(tong_sl_chitl) tong_sl_chitl,
			
				 sum(sl_brcd_chuachitl) sl_brcd_chuachitl,
				 sum(sl_mytv_chuachitl) sl_mytv_chuachitl,
				 sum(sl_vnpts_chuachitl) sl_vnpts_chuachitl,
				 sum(sl_vnptt_chuachitl) sl_vnptt_chuachitl,
				 sum(tong_sl_chuachitl) tong_sl_chuachitl,
			
				 sum(sl_thuhoi_brcd) sl_thuhoi_brcd,
				 sum(sl_thuhoi_mytv) sl_thuhoi_mytv,
				 sum(sl_thuhoi_vnpts) sl_thuhoi_vnpts,
				 sum(sl_thuhoi_vnptt) sl_thuhoi_vnptt,
				 sum(tong_sl_thuhoi) tong_sl_thuhoi      
			from ttkd_bsc.bangluong_dongia_potmasco
			where thang = 202406
							and (tong_tl is not null or tong_dt_chuachitl is not null or tong_tl_thuhoi is not null)
			
			;



-- BBXN-DTPTM-THULAO_NV - Tong hop thu lao theo NVVT PTM:
		select rownum, ma_nv, ten_nv, ten_to, ten_pb,
			   dt_brcd_chitl, dt_mytv_chitl, dt_vnpts_chitl, dt_vnptt_chitl, tong_dt_chitl, 
			   dt_brcd_chuachitl, dt_mytv_chuachitl, dt_vnpts_chuachitl, dt_vnptt_chuachitl, tong_dt_chuachitl, 
			   tl_brcd, tl_mytv, tl_vnpts, tl_vnptt, tong_tl, 
			   sl_brcd_chitl, sl_mytv_chitl, sl_vnpts_chitl, sl_vnptt_chitl, tong_sl_chitl,
			   sl_brcd_chuachitl, sl_mytv_chuachitl, sl_vnpts_chuachitl, sl_vnptt_chuachitl, tong_sl_chuachitl,
			   tong_tl_thuhoi, tong_sl_thuhoi
		from ttkd_bsc.bangluong_dongia_potmasco
		where thang = 202406
							and (tong_tl is not null or tong_dt_chuachitl is not null or tong_tl_thuhoi is not null)
		order by 1
		;
		---File tonghop_chitiet__nhanvien_0x202x.xlsx
			select ma_nv, ten_nv, ten_to, ten_pb,
						 tong_tl tong_thulao
						   , tong_tl_thuhoi tong_thuhoi, nvl(tong_tl, 0) - nvl(tong_tl_thuhoi, 0)  Thulao_chitra
						   , substr(ma_nv, 1,3) ghichu
		from ttkd_bsc.bangluong_dongia_potmasco
		where thang = 202406
						and (tong_tl is not null or tong_tl_thuhoi is not null)
		order by 4,3,2
		;

-- File excel chitiet_tbao_thulao_thuhoi_XXyyyy: (bao gom da tinh, chua tinh v� thu hoi) 
	select *
	from(
				select 'PTM' nguon, thang_ptm
						, ma_gd, ma_tb, dich_vu, ngay_bbbg
				         , lydo_khongtinh_dongia lydo_chuachi
					    , manv_ptm, b.ten_nv, b.ma_to, b.ten_to, b.ma_pb, b.ten_pb
					    , (case when thang_tldg_dnhm = 202406 then doanhthu_dongia_dnhm else 0 end) -- dt_dongia_dnhm
							+ (case when thang_tldg_dt = 202406 then doanhthu_dongia_nvptm else 0 end) --dt_dongia_goi
							as tong_dt_dongia_goi
						, (case when thang_tldg_dnhm = 202406 then luong_dongia_dnhm_nvptm else 0 end)  --tl_dnhm
							+ (case when thang_tldg_dt = 202406 then luong_dongia_nvptm else 0 end) --tl_goi
							as tong_tl_goi
--				 select * 
				from ttkd_bsc.ct_bsc_ptm_potmasco_202406 a
							join ttkd_bsc.nhanvien_vttp_potmasco b on b.thang = 202406 and a.manv_ptm = b.ma_nv     --thang n
				where loaitb_id in (58, 61, 20, 11)
				
				union all
				select 'PTM' nguon, thang_ptm
						, ma_gd, ma_tb, dich_vu, ngay_bbbg
				         , lydo_khongtinh_dongia lydo_chuachi
					    , manv_hotro, b.ten_nv, b.ma_to, b.ten_to, b.ma_pb, b.ten_pb
					    , (case when thang_tldg_dnhm = 202406 then doanhthu_dongia_dnhm else 0 end) -- dt_dongia_dnhm
							+ (case when thang_tldg_dt = 202406 then doanhthu_dongia_nvptm else 0 end) --dt_dongia_goi
							as tong_dt_dongia_goi
						, (case when thang_tldg_dnhm = 202406 then luong_dongia_dnhm_nvptm else 0 end)  --tl_dnhm
							+ (case when thang_tldg_dt = 202406 then luong_dongia_nvptm else 0 end) --tl_goi
							as tong_tl_goi
--				 select * 
				from ttkd_bsc.ct_bsc_ptm_potmasco_202406 a
							join ttkd_bsc.nhanvien_vttp_potmasco b on b.thang = 202406 and a.manv_hotro = b.ma_nv     --thang n
				where loaitb_id in (58, 61, 20, 11)
				
				union all
				select 'PTM' nguon, thang_ptm
						, ma_gd, ma_tb, dich_vu, ngay_bbbg
				         , lydo_khongtinh_dongia lydo_chuachi
					    , a.manv_ptm, b.ten_nv, b.ma_to, b.ten_to, b.ma_pb, b.ten_pb
						 , (case when thang_tldg_dnhm = 202406 then tien_dnhm else 0 end) -- dt_dongia_dnhm
							+ (case when thang_tldg_dt = 202406 then dthu_goi_goc else 0 end) --dt_dongia_goi
							as tong_dt_dongia_goi
						, (case when thang_tldg_dnhm = 202406 then luong_dongia_dnhm_nvptm else 0 end)  --tl_dnhm
							+ (case when thang_tldg_dt = 202406 then luong_dongia_nvptm else 0 end) --tl_goi
							as tong_tl_goi
				from ttkd_bsc.ct_bsc_ptm_potmasco_202406 a
								join ttkd_bsc.nhanvien_vttp_potmasco b on b.thang = 202406 and a.manv_ptm = b.ma_nv     --thang n
				where a.loaitb_id=21
		) where TONG_DT_DONGIA_GOI is not null  or TONG_TL_GOI is not null
		
		
		union all

-- CT_THUHOI
		select 'Thuhoi' nguon, cast(thang_ptm as number) thang
				, null, ma_tb, null, null
				 , loai_thu ||'; '|| ghichu lydo_chuachi
			 , a.ma_nv, b.ten_nv, b.ma_to, b.ten_to, b.ma_pb, b.ten_pb
			 ,doanhthu_dongia,tien_thuhoi 
		 from ttkd_bsc.ct_thuhoi a
						join ttkd_bsc.nhanvien_vttp_potmasco b on a.thang = b.thang and a.ma_nv = b.ma_nv     --thang n
		 where a.thang = 202406 and a.loaitb_id in (58, 61, 20, 21) 
		--   and exists(select * from ttkd_bsc.bangluong_dongia_202406_vttp where ma_nv=a.ma_nv)
   ;

select * from  ttkd_bsc.ct_thuhoi
;
select ma_nv, count(ma_to) from ttkd_bsc.ct_bsc_ptm_202406_vttp where thang=202406 
group by ma_nv;





