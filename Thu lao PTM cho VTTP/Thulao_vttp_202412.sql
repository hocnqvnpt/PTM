-- Chi tiet trong ky:
	---chi tinh 5 loai hinh: VNPtt, VNPts, Fiber, MyTV, Mega
	---thang_tldg_dt : thang duoc tinh luong don gia (null: chua tinh, 999999 khong dc tinh, yyyymm thang dc tinh)
	---thang_ptm: thang insert table
	---nhom_tiepthi: tinh nhan vien VTTP (danh muc ttkd_bsc.dm_nhomld key nhomld_id)
	---thoi gian tinh trong vong 4 thang tinh tu thang_ptm, rieng VNPtt tinh 1 thang
	---VTTP chi tinh PTM, TTKD tinh PTM, nang toc do, tai lap
drop table ttkd_bsc.ct_bsc_ptm_202412_vttp;

select * from ttkd_bsc.ct_bsc_ptm_202412_vttp;
            
		  create table ttkd_bsc.ct_bsc_ptm_202412_vttp as
                select * from ttkd_bsc.ct_bsc_ptm
                where 
							(manv_ptm in (select ma_nv from ttkd_bsc.nhanvien where thang = 202412 and donvi = 'VTTP')
								or manv_hotro in (select ma_nv from ttkd_bsc.nhanvien where thang = 202412 and donvi = 'VTTP')
							)
                                and ((thang_ptm >= 202409     ---thang n-3
										  and ((loaihd_id = 1 and loaitb_id in (11, 58, 61)) or loaitb_id=20)
										  and (thang_tldg_dt = 202412 or thang_tldg_dnhm = 202412 or thang_tldg_dt is null))          ---thang n
                                            or (loaitb_id = 21 and thang_ptm = 202412)
								    )
								                           ---thang n
    ;
--select * from ttkd_bsc.nhanvien where donvi = 'VTTP' and thang = 202412;
--select * from  ttkd_bsc.bangluong_dongia_vttp where thang = 202412;

insert into ttkd_bsc.bangluong_dongia_vttp
      (ma_nv,ten_nv,ma_vtcv,ten_vtcv,ma_pb,ten_pb,ma_to,ten_to,thang )
		SELECT ma_nv,ten_nv,ma_vtcv,ten_vtcv,ma_pb,ten_pb,ma_to,ten_to, thang           --thang n
		  FROM ttkd_bsc.nhanvien a
		  where donvi = 'VTTP' and thang = 202412
--					exists(select 1 from ttkd_bsc.ct_bsc_ptm_202412_vttp where manv_ptm=a.ma_nv)
--						and thang=(select max(thang) from ttkd_bsc.nhanvien_vttp where ma_nv=a.ma_nv)
	   ;
		commit;
    
-- tong hop:  
		drop table ttkd_bsc.x_th_tlptm_vttp
		;
					create table ttkd_bsc.x_th_tlptm_vttp as
					    -- dthu + thu lao duoc tinh cdbr, mytv, vnpts
					    select  ma_nv, ten_pb
									  , count(distinct case when loaitb_id in (11,58) then ma_tb end) sl_brcd_chitl
									  , count(distinct case when loaitb_id = 61 then ma_tb end) sl_mytv_chitl
									  , count(distinct case when loaitb_id = 20  then ma_tb end) sl_vnpts_chitl
									  , count(distinct case when loaitb_id = 21  then ma_tb end) as sl_vnptt_chitl
									  
									  , sum(case when loaitb_id in (11,58) then doanhthu_dongia else 0 end) dt_brcd_chitl
									  , sum(case when loaitb_id=61 then doanhthu_dongia else 0 end) dt_mytv_chitl
									  , sum(case when loaitb_id=20  then doanhthu_dongia else 0 end) dt_vnpts_chitl
									  , sum(case when loaitb_id = 21  then doanhthu_dongia else 0 end) dt_vnptt_chitl
							
									  , sum(case when loaitb_id in (11, 58) then tien_thulao else 0 end) tl_brcd
									  , sum(case when loaitb_id = 61 then tien_thulao else 0 end) tl_mytv
									  , sum(case when loaitb_id = 20  then tien_thulao else 0 end) tl_vnpts
									  , sum(case when loaitb_id = 21  then tien_thulao else 0 end) tl_vnptt		---khong nhan don gia doanh thu vi vb rieng tinh theo %
					
					    from ttkd_bsc.tonghop_ct_dongia_ptm
					    where thang = 202412 and donvi = 'VTTP'          ---thang n---
					    group by ma_nv, ten_pb
				
;
-- sltb + dthu goi chua duoc tinh
		drop table ttkd_bsc.x_th_tlptm_vttp_chuachitl;
--		select * from ttkd_bsc.x_th_tlptm_vttp_chuachitl;
;
					create table ttkd_bsc.x_th_tlptm_vttp_chuachitl as
					select 						ma_nv, ten_pb
										, count(distinct case when loaitb_id in (11,58) then ma_tb end) sl_brcd_chuachitl
										, count(distinct case when loaitb_id=61 then ma_tb end) sl_mytv_chuachitl
										, count(distinct case when loaitb_id=20  then ma_tb end) sl_vnpts_chuachitl
										, count(distinct case when loaitb_id = 21  then ma_tb end)  sl_vnptt_chuachitl
										  
										, sum(case when loaitb_id in (11,58) then doanhthu_dongia else 0 end) dt_brcd_chuachitl
										, sum(case when loaitb_id=61 then doanhthu_dongia else 0 end) dt_mytv_chuachitl
										, sum(case when loaitb_id=20  then doanhthu_dongia else 0 end) dt_vnpts_chuachitl
										, sum(case when loaitb_id = 21  then doanhthu_dongia else 0 end)  dt_vnptt_chuachitl
										
										, sum(case when loaitb_id in (11, 58) then tien_thulao else 0 end) tl_brcd
										, sum(case when loaitb_id = 61 then tien_thulao else 0 end) tl_mytv
										, sum(case when loaitb_id = 20  then tien_thulao else 0 end) tl_vnpts
										, sum(case when loaitb_id = 21  then tien_thulao else 0 end) tl_vnptt		---khong nhan don gia doanh thu vi vb rieng tinh theo %
			from
			(select 
							nv.thang, nv.donvi, manv_ptm ma_nv, nv.ten_nv, nv.ten_to, nv.ten_pb, nv.ma_to, nv.ma_pb, nv.ma_vtcv, nv.nhomld_id
							, ptm_id, THANG_PTM, MA_GD, thuebao_id, MA_TB, LOAITB_ID, DICHVUVT_ID
							, doanhthu_dongia
							, dongia
							, LUONG_DONGIA_CDBR, LUONG_DONGIA_VNPTS, LUONG_DONGIA_KHAC
							, case when nguon = 'dnhm' then LUONG_DONGIA_VNPTT end LUONG_DONGIA_dnhm_VNPTT
							, case when nguon = 'nvptm' then LUONG_DONGIA_VNPTT end LUONG_DONGIA_goi_VNPTT
							, luong_dongia
							, case 
										when loaitb_id = 21 and nv.donvi = 'VTTP' then 1				---VTTP VNPtt heso = 1
										when nv.donvi = 'VTTP' and thang_ptm < 202407 then 1 	---fix truoc thang 7 theo vb 353 don gia = 858, from thang 7 theo vb 322 thay doi don gia 0.8 * 800 = 640
										when nv.donvi = 'VTTP' and thang_ptm >= 202407 and thang_ptm <= 202411 then 0.8 	---fix truoc thang 7 theo vb 353 don gia = 858, from thang 7 theo vb 322 thay doi don gia 0.8 * 800 = 640
										else hs.heso_dthu end heso_dthu
							, case 
										when loaitb_id = 21 and nv.donvi = 'VTTP' then luong_dongia
										when nv.donvi = 'VTTP' and thang_ptm < 202407 then luong_dongia 	---fix truoc thang 7 theo vb 353 don gia = 858, from thang 7 theo vb 322 thay doi don gia 0.8 * 800 = 640
										when nv.donvi = 'VTTP' and thang_ptm >= 202407 and thang_ptm <= 202411 then luong_dongia * 0.8 	---fix truoc thang 7 theo vb 353 don gia = 858, from thang 7 theo vb 322 thay doi don gia 0.8 * 800 = 640
												else luong_dongia * hs.heso_dthu end tien_thulao
							, NGUON
			from

					(
		--			create table x_tpmp as	
					select  id ptm_id, ma_gd, thuebao_id, ma_tb, loaitb_id, dichvuvt_id, manv_ptm
									  , case when dichvuvt_id not in (2,13,14,15,16) then doanhthu_dongia_nvptm end dthu_dongia_cdbr
									  , case when loaitb_id=20 then doanhthu_dongia_nvptm end dthu_dongia_vnpts
									  , case when loaitb_id=21  then doanhthu_dongia_nvptm end dthu_dongia_vnptt
									  , case when dichvuvt_id in (13,14,15,16) or dichvuvt_id is null then doanhthu_dongia_nvptm end dthu_dongia_khac
									  , doanhthu_dongia_nvptm doanhthu_dongia
									  , dongia
									  , case when dichvuvt_id not in (2,13,14,15,16) then luong_dongia_nvptm end luong_dongia_cdbr
									  , case when loaitb_id = 20  then luong_dongia_nvptm end luong_dongia_vnpts
									  , case when loaitb_id = 21  then luong_dongia_nvptm end luong_dongia_vnptt
									  , case when dichvuvt_id in (13,14,15,16) or loaitb_id is null then luong_dongia_nvptm end luong_dongia_khac
									  , luong_dongia_nvptm luong_dongia
									  ,'nvptm' nguon, thang_tldg_dt thang_dongia, thang_ptm
					    from ttkd_bsc.ct_bsc_ptm
					    where manv_ptm is not null and thang_ptm >= 202409 and thang_tldg_dt is null
									and loaitb_id <> 21 		---vi khong su dung cọt thang_tldg_dt
				union all     
					    -- dnhm cua nvhotro tao hop dong (phan chia nvien hotro DIGISHOP)
					    select  id, ma_gd, thuebao_id, ma_tb, loaitb_id, dichvuvt_id, manv_ptm
									  , case when dichvuvt_id not in (2,13,14,15,16) then doanhthu_dongia_dnhm * heso_hotro_nvptm end dthu_dongia_cdbr
									  , case when loaitb_id = 20 then doanhthu_dongia_dnhm * heso_hotro_nvptm end dthu_dongia_vnpts
									  , case when loaitb_id = 21 then doanhthu_dongia_dnhm end dthu_dongia_vnptt
									  , case when loaitb_id in (38,127) then doanhthu_dongia_dnhm * heso_hotro_nvptm end  dthu_dongia_khac
									  , doanhthu_dongia_dnhm * nvl(heso_hotro_nvptm, 1) doanhthu_dongia
									  , dongia
									  , case when dichvuvt_id not in (2,13,14,15,16) then luong_dongia_dnhm_nvptm * heso_hotro_nvptm end luong_dongia_cdbr
									  , case when loaitb_id =20 then luong_dongia_dnhm_nvptm * heso_hotro_nvptm end luong_dongia_vnpts
									  , case when loaitb_id =21 then luong_dongia_dnhm_nvptm end luong_dongia_vnptt
									  , case when loaitb_id in (38,127) then luong_dongia_dnhm_nvptm * heso_hotro_nvptm end luong_dongia_khac
									  , luong_dongia_dnhm_nvptm * nvl(heso_hotro_nvptm, 1) luong_dongia
									 , 'dnhm' nguon, thang_tldg_dnhm, thang_ptm
					    from ttkd_bsc.ct_bsc_ptm
					    where manv_ptm is not null and thang_ptm >= 202409 and thang_tldg_dnhm is null
				union all			    
					     -- dnhm cua nvptm (phan chia nvien tiep thi DIGISHOP)
					    select  id, ma_gd, thuebao_id, ma_tb, loaitb_id, dichvuvt_id, manv_hotro
									  , case when dichvuvt_id not in (2,13,14,15,16) then doanhthu_dongia_dnhm * heso_hotro_nvhotro end dthu_dongia_cdbr
									  , case when loaitb_id = 20 then doanhthu_dongia_dnhm * heso_hotro_nvhotro end dthu_dongia_vnpts
									  , case when loaitb_id = 21 then doanhthu_dongia_dnhm * heso_hotro_nvhotro end dthu_dongia_vnptt
									  , case when loaitb_id in (38,127) then doanhthu_dongia_dnhm * heso_hotro_nvhotro end  dthu_dongia_khac
									  , doanhthu_dongia_dnhm * nvl(heso_hotro_nvhotro, 1) doanhthu_dongia
									  , dongia
									  , case when dichvuvt_id not in (2,13,14,15,16) then luong_dongia_dnhm_nvptm * heso_hotro_nvhotro end luong_dongia_cdbr
									  , case when loaitb_id = 20 then luong_dongia_dnhm_nvptm * heso_hotro_nvhotro end luong_dongia_vnpts
									  , case when loaitb_id = 21 then luong_dongia_dnhm_nvptm * heso_hotro_nvhotro end luong_dongia_vnptt
									  , case when loaitb_id in (38,127) then luong_dongia_dnhm_nvptm * heso_hotro_nvhotro end luong_dongia_khac
									  , luong_dongia_dnhm_nvptm * nvl(heso_hotro_nvhotro, 1) luong_dongia
									 , 'dnhm' nguon, thang_tldg_dnhm, thang_ptm
					    from ttkd_bsc.ct_bsc_ptm
					    where manv_hotro is not null and thang_ptm >= 202409 and thang_tldg_dnhm is null 
										and tyle_hotro is null and tyle_am is null and nvl(vanban_id, 0) != 764 ---only T7 xoa
				union all     
					    -- nv dai
					    select  id, ma_gd, thuebao_id, ma_tb, loaitb_id, dichvuvt_id, manv_tt_dai
							  , case when dichvuvt_id not in (2,13,14,15,16) then doanhthu_dongia_dai end dthu_dongia_cdbr
							  , case when loaitb_id=20  then doanhthu_dongia_dai end dthu_dongia_vnpts
							  , case when loaitb_id=21  then doanhthu_dongia_dai end dthu_dongia_vnptt
							  , case when dichvuvt_id  in (13,14,15,16) or dichvuvt_id is null then doanhthu_dongia_dai end dthu_dongia_khac
							  , doanhthu_dongia_dai doanhthu_dongia
							  , dongia
							  , case when dichvuvt_id not in (2,13,14,15,16) then luong_dongia_dai end luong_dongia_cdbr
							  , case when loaitb_id=20  then luong_dongia_dai end luong_dongia_vnpts
							 , case when loaitb_id = 21  then luong_dongia_dai end luong_dongia_vnptt
							  , case when (dichvuvt_id  in (13,14,15,16)  or dichvuvt_id is null) then luong_dongia_dai end luong_dongia_khac
							  , luong_dongia_dai luong_dongia
							, 'nvtt_dai' nguon, thang_tldg_dt_dai, thang_ptm
					    from ttkd_bsc.ct_bsc_ptm
					    where manv_tt_dai is not null and thang_ptm >= 202409 and thang_tldg_dt_dai is null
				union all     
					    -- nv gioi thieu DIGISHOP, ngoai tru  phong GP
					    select  id, ma_gd, thuebao_id, ma_tb, loaitb_id, dichvuvt_id, manv_hotro
										  , case when dichvuvt_id not in (2,13,14,15,16) then doanhthu_dongia_nvhotro end dthu_dongia_cdbr
										  , case when loaitb_id=20  then doanhthu_dongia_nvhotro end dthu_dongia_vnpts
										  , case when loaitb_id=21  then doanhthu_dongia_nvhotro end dthu_dongia_vnptt
										  , case when dichvuvt_id  in (13,14,15,16) or dichvuvt_id is null then doanhthu_dongia_nvhotro end dthu_dongia_khac
										  , doanhthu_dongia_nvhotro doanhthu_dongia
										  , dongia
										  , case when dichvuvt_id not in (2,13,14,15,16) then luong_dongia_nvhotro end luong_dongia_cdbr
										  , case when loaitb_id=20  then luong_dongia_nvhotro end luong_dongia_vnpts
										 , case when loaitb_id=21  then luong_dongia_nvhotro end luong_dongia_vnptt
										  , case when (dichvuvt_id  in (13,14,15,16)  or dichvuvt_id is null) then luong_dongia_nvhotro end luong_dongia_khac
										  , luong_dongia_nvhotro luong_dongia
										,'nvtt_gioithieu' nguon, thang_tldg_dt_nvhotro, thang_ptm
					    from ttkd_bsc.ct_bsc_ptm a
					    where manv_hotro is not null and thang_ptm >= 202409 and thang_tldg_dt_nvhotro is null
									and not exists (select * from ttkd_bsc.nhanvien where thang = 202412 and ma_pb = 'VNP0702600' and ma_nv = a.manv_hotro)				
				 ) a
			    
			join ttkd_bsc.nhanvien nv on nv.donvi = 'VTTP' and nv.thang = 202412 and nv.ma_nv = a.manv_ptm
			left join ttkd_bsc.bang_heso_dthu hs on hs.thang = a.thang_ptm and a.manv_ptm = hs.ma_nv
			)
			group by ma_nv, ten_pb
		;
    
 
-- ptm cdbr + gtgt +vnpts             
			update ttkd_bsc.bangluong_dongia_vttp a
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
				where thang = 202412
	;
    
            
update ttkd_bsc.bangluong_dongia_vttp a
            set ( sl_brcd_chitl, sl_mytv_chitl, sl_vnpts_chitl, sl_vnptt_chitl, 
                    dt_brcd_chitl, dt_mytv_chitl, dt_vnpts_chitl, dt_vnptt_chitl,
                    tl_brcd, tl_mytv, tl_vnpts, tl_vnptt)
           =
		 (select sl_brcd_chitl, sl_mytv_chitl, sl_vnpts_chitl, sl_vnptt_chitl, 
                    dt_brcd_chitl, dt_mytv_chitl, dt_vnpts_chitl, dt_vnptt_chitl,
                    tl_brcd, tl_mytv, tl_vnpts, tl_vnptt
             from ttkd_bsc.x_th_tlptm_vttp
             where ma_nv = a.ma_nv)
--     select * from ttkd_bsc.bangluong_dongia_vttp a
    where thang = 202412
				and exists(select 1 from ttkd_bsc.x_th_tlptm_vttp where ma_nv=a.ma_nv)
    
    
    ;


update ttkd_bsc.bangluong_dongia_vttp a
            set (sl_brcd_chuachitl, sl_mytv_chuachitl, sl_vnpts_chuachitl, sl_vnptt_chuachitl,
                    dt_brcd_chuachitl, dt_mytv_chuachitl, dt_vnpts_chuachitl, dt_vnptt_chuachitl)
            =(select sl_brcd_chuachitl, sl_mytv_chuachitl, sl_vnpts_chuachitl, sl_vnptt_chuachitl,
                    dt_brcd_chuachitl, dt_mytv_chuachitl, dt_vnpts_chuachitl, dt_vnptt_chuachitl
              from ttkd_bsc.x_th_tlptm_vttp_chuachitl
              where ma_nv=a.ma_nv)
--     select * from ttkd_bsc.bangluong_dongia_vttp a
    where thang = 202412
				and exists(select 1 from ttkd_bsc.x_th_tlptm_vttp_chuachitl where ma_nv=a.ma_nv)
    
    ;

update ttkd_bsc.bangluong_dongia_vttp set tong_dt_chitl='', tong_tl='' , tong_dt_chuachitl='',  tong_sl_chitl='', tong_sl_chuachitl='' where thang = 202412;
update ttkd_bsc.bangluong_dongia_vttp a
            set  tong_dt_chitl      = nvl(dt_brcd_chitl,0)+nvl(dt_mytv_chitl,0)+nvl(dt_vnpts_chitl,0)+nvl(dt_vnptt_chitl,0)
                    ,tong_tl                = round(nvl(tl_brcd,0)+nvl(tl_mytv,0)+nvl(tl_vnpts,0)+nvl(tl_vnptt,0), 0)
                    ,tong_dt_chuachitl = nvl(dt_brcd_chuachitl,0)+nvl(dt_mytv_chuachitl,0)+nvl(dt_vnpts_chuachitl,0)+nvl(dt_vnptt_chuachitl,0)
                    ,tong_sl_chitl      = nvl(sl_brcd_chitl,0)+nvl(sl_mytv_chitl,0)+nvl(sl_vnpts_chitl,0)+nvl(sl_vnptt_chitl,0)   
                    ,tong_sl_chuachitl = nvl(sl_brcd_chuachitl,0)+nvl(sl_mytv_chuachitl,0)+nvl(sl_vnpts_chuachitl,0)+nvl(sl_vnptt_chuachitl,0)
	where thang = 202412
				;
--				select * from ttkd_bsc.bangluong_dongia_vttp where thang = 202412;


            commit;
-- Thu hoi:
				update ttkd_bsc.bangluong_dongia_vttp a  
				   set tl_thuhoi_brcd = '', tl_thuhoi_mytv = '', tl_thuhoi_vnpts = '', tl_thuhoi_vnptt = '', tong_tl_thuhoi = ''
						, sl_thuhoi_brcd='', sl_thuhoi_mytv='', sl_thuhoi_vnpts='', sl_thuhoi_vnptt='', tong_sl_thuhoi=''
				   where tong_tl_thuhoi is not null
								and thang = 202412
				   ;
--   select * from ttkd_bsc.bangluong_dongia_202412_vttp a
				update ttkd_bsc.bangluong_dongia_vttp a  
				   set (tl_thuhoi_brcd, tl_thuhoi_mytv, tl_thuhoi_vnpts, tl_thuhoi_vnptt, tong_tl_thuhoi, sl_thuhoi_brcd, sl_thuhoi_mytv, sl_thuhoi_vnpts, sl_thuhoi_vnptt, tong_sl_thuhoi) 
							 = (select sum(case when loaitb_id=58 then tien_thuhoi else 0 end)
										  , sum(case when loaitb_id=61 then tien_thuhoi else 0 end)
										  , sum(case when loaitb_id=20 then tien_thuhoi else 0 end)
										  , sum(case when loaitb_id=21 then tien_thuhoi else 0 end)
										  , sum(case when loaitb_id in (58, 61, 20, 21) then tien_thuhoi else 0 end)
										  , sum(case when loaitb_id=58 then 1 end)
										  , sum(case when loaitb_id=61 then 1 end)
										  , sum(case when loaitb_id=20 then 1 end)
										  , sum(case when loaitb_id=21 then 1 end)
										  ,count(case when loaitb_id in (58, 61, 20, 21) then 1 end)
								  from ttkd_bsc.ct_thuhoi  
								  where thang = 202412 and ma_nv =a.ma_nv)             ---thang n
				   where exists(select 1 from ttkd_bsc.ct_thuhoi 
									   where thang = 202412 and ma_nv = a.ma_nv)                         ---thang n
				;
				update ttkd_bsc.bangluong_dongia_vttp a
					  set  tong_tl_thuhoi      = nvl(tong_tl_thuhoi,0)
							, tong_sl_thuhoi     = nvl(tong_sl_thuhoi,0)
				where thang = 202412
				;				
			
			delete from ttkd_bsc.bangluong_dongia_vttp where thang = 202412
			    and nvl(tong_tl, 0) =0 and nvl(tong_dt_chuachitl, 0) = 0 and nvl(tong_sl_chitl, 0) = 0 and nvl(tong_sl_chuachitl, 0) = 0 and nvl(tong_sl_thuhoi, 0) = 0;
    
commit;

select * from ttkd_bsc.bangluong_dongia_vttp where thang = 202412;

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
				from ttkd_bsc.bangluong_dongia_vttp
				where thang = 202412
								and (nvl(tong_tl, 0) >0 or nvl(tong_dt_chuachitl, 0) >0 or nvl(tong_tl_thuhoi, 0) > 0)
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
				from ttkd_bsc.bangluong_dongia_vttp
				where thang = 202412
								and (nvl(tong_tl, 0) >0 or nvl(tong_dt_chuachitl, 0) >0 or nvl(tong_tl_thuhoi, 0) > 0)

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
			from ttkd_bsc.bangluong_dongia_vttp
			where thang = 202412
							and (nvl(tong_tl, 0) >0 or nvl(tong_dt_chuachitl, 0) >0 or nvl(tong_tl_thuhoi, 0) > 0)
			group by ten_pb
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
			from ttkd_bsc.bangluong_dongia_vttp
			where thang = 202412
							and (nvl(tong_tl, 0) >0 or nvl(tong_dt_chuachitl, 0) >0 or nvl(tong_tl_thuhoi, 0) > 0)
			
			;



-- BBXN-DTPTM-THULAO_NV - Tong hop thu lao theo NVVT PTM:
		select rownum, ma_nv, ten_nv, ten_to, ten_pb,
			   dt_brcd_chitl, dt_mytv_chitl, dt_vnpts_chitl, dt_vnptt_chitl, tong_dt_chitl, 
			   dt_brcd_chuachitl, dt_mytv_chuachitl, dt_vnpts_chuachitl, dt_vnptt_chuachitl, tong_dt_chuachitl, 
			   tl_brcd, tl_mytv, tl_vnpts, tl_vnptt, tong_tl, 
			   sl_brcd_chitl, sl_mytv_chitl, sl_vnpts_chitl, sl_vnptt_chitl, tong_sl_chitl,
			   sl_brcd_chuachitl, sl_mytv_chuachitl, sl_vnpts_chuachitl, sl_vnptt_chuachitl, tong_sl_chuachitl,
			   tong_tl_thuhoi, tong_sl_thuhoi
		from ttkd_bsc.bangluong_dongia_vttp
		where thang = 202412
							and (nvl(tong_tl, 0) >0 or nvl(tong_dt_chuachitl, 0) >0 or nvl(tong_tl_thuhoi, 0) > 0)
		order by 1
		;
		---File tonghop_chitiet__nhanvien_0x202x.xlsx
			select 
						ma_nv, ten_nv, ten_to, ten_pb
						 , tong_tl tong_thulao
						, tong_tl_thuhoi tong_thuhoi, nvl(tong_tl, 0) - nvl(tong_tl_thuhoi, 0)  Thulao_chitra
						, substr(ma_nv, 1,3) ghichu
--						sum(tong_tl)
		from ttkd_bsc.bangluong_dongia_vttp
		where thang = 202412
						and (nvl(tong_tl, 0) >0 or nvl(tong_tl_thuhoi, 0) > 0)
		order by 4,3,2
		;

-- File excel chitiet_tbao_thulao_thuhoi_xx2024:

					select 'PTM_chuachi' nguon, thang_ptm
										, ma_gd, ma_tb, dich_vu, ngay_bbbg, ngay_luuhs_ttkd, ngay_luuhs_ttvt, nop_du
									    , lydo_khongtinh_dongia lydo_chuachi, thang_tldg_dt thang_chi
									    , manv_ptm, b.ten_nv, b.ma_to, b.ten_to, b.ma_pb, b.ten_pb
									    , (case when doanhthu_dongia_nvptm > 0 then doanhthu_dongia_nvptm else 0 end) --dt_dongia_goi
											as tong_dt_dongia_goi
										, dongia, luong_dongia_nvptm, case when dongia = 800 then 0.8 else 1 end heso_dthu
										, (case when dongia = 800 then luong_dongia_nvptm * 0.8
														when dongia = 858 then luong_dongia_nvptm else 0 end) --tl_goi
											as tong_tl_goi
						-- select * 
						from ttkd_bsc.ct_bsc_ptm_202412_vttp a
									join ttkd_bsc.nhanvien b on b.thang = 202412 and donvi = 'VTTP' and a.manv_ptm = b.ma_nv     --thang n
						where loaitb_id in (58, 61, 20, 11, 21) and thang_tldg_dt is null
						
						union all
						select 'PTM_DNHM_chuachi' nguon, thang_ptm
										, ma_gd, ma_tb, dich_vu, ngay_bbbg, ngay_luuhs_ttkd, ngay_luuhs_ttvt, nop_du
									    , lydo_khongtinh_dongia lydo_chuachi, thang_tldg_dt thang_chi
									    , manv_ptm, b.ten_nv, b.ma_to, b.ten_to, b.ma_pb, b.ten_pb
									    , (case when doanhthu_dongia_dnhm > 0 then doanhthu_dongia_dnhm else 0 end) --dt_dongia_goi
											as doanhthu_dongia_dnhm
										, dongia, luong_dongia_dnhm_nvptm, case when dongia = 800 then 0.8 else 1 end heso_dthu
										, (case when dongia = 800 then luong_dongia_dnhm_nvptm * 0.8
														when dongia = 858 then luong_dongia_dnhm_nvptm else 0 end) --tl_goi
											as tong_tl_goi
						-- select * 
						from ttkd_bsc.ct_bsc_ptm_202412_vttp a
									join ttkd_bsc.nhanvien b on b.thang = 202412 and donvi = 'VTTP' and a.manv_ptm = b.ma_nv     --thang n
						where loaitb_id in (58, 61, 20, 11, 21) and thang_tldg_dnhm is null and doanhthu_dongia_dnhm > 0
		
		union all
				select nguon, thang_ptm, ma_gd, ma_tb, lh.loaihinh_tb, null, null, null, null
							,null, thang thang_chi
							, MA_NV, TEN_NV, ma_to, TEN_TO, ma_pb, TEN_PB, DOANHTHU_DONGIA, DONGIA, LUONG_DONGIA, HESO_DTHU, TIEN_THULAO
						from ttkd_bsc.tonghop_ct_dongia_ptm a
									left join css_hcm.loaihinh_tb lh on a.loaitb_id = lh.loaitb_id
					    where a.loaitb_id in (58, 61, 20, 11, 21) and thang = 202412 and donvi = 'VTTP'

		union all

-- CT_THUHOI
		select 'Thuhoi' nguon, cast(thang_ptm as number) thang_ptm
				, null, ma_tb, null, null, null, null, null
				 , loai_thu ||'; '|| ghichu lydo_chuachi, cast(a.thang as number)  thang_thuhoi
			 , a.ma_nv, b.ten_nv, b.ma_to, b.ten_to, b.ma_pb, b.ten_pb
			 ,doanhthu_dongia, null, null, null, tien_thuhoi 
--		select *
		 from ttkd_bsc.ct_thuhoi a, ttkd_bsc.nhanvien b
		 where a.thang=202412 and a.loaitb_id in (58, 61, 20, 11, 21) 
						and a.ma_nv=b.ma_nv and a.thang = b.thang and b.donvi = 'VTTP'
		--   and exists(select * from ttkd_bsc.bangluong_dongia_202412_vttp where ma_nv=a.ma_nv)
;
select * from  ttkd_bsc.ct_thuhoi


