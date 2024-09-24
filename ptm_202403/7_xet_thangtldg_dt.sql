rollback;
-- ID447: cap nhat ktoan kh xac nhan thu tien
	update ttkd_bsc.ct_bsc_ptm a 
		   set (thang_luong, xacnhan_khkt, thang_xacnhan_khkt, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, thang_tldg_dt_nvhotro, thang_tlkpi_hotro) =       
				 (select 7, tienthu_khkt, to_char(ngaycapnhat_khkt,'yyyymm') , 202404, 202404, 202404, 202404, 
					    case when manv_hotro is not null then 202404 end ,
					    case when manv_hotro is not null then 202404 end
					from ttkdhcm_ktnv.amas_duan_ngoai_doanhthu where id=a.id_447)
	    -- select * from ttkd_bsc.ct_bsc_ptm a
	    where  thang_ptm>=202401 and thang_xacnhan_khkt is null ---thang n-3
			and exists(select 1 from ttkdhcm_ktnv.amas_duan_ngoai_doanhthu where ngaythanhtoan_khkt is not null and id=a.id_447)
			;
   ---1 dot 1, 2
   
 
-- Ptm qua Digishop:
		update ttkd_bsc.ct_bsc_ptm a 
		    set thang_tldg_dt=999999, thang_tldg_dnhm=999999
	---select thang_tldg_dt, thang_tldg_dnhm from ttkd_bsc.ct_bsc_ptm a 
		    where thang_ptm=202404 and ungdung_id=17 and (nhom_tiepthi<>2 or nhom_tiepthi is null)
		    ;
        
       ---2  dot 1, 2
-- ******************Ky n ***************** 
			---chay khi co yeu cau
--			update ttkd_bsc.ct_bsc_ptm set thang_tldg_dt=null
--			    where thang_ptm=202404 and (loaitb_id<>21 or ma_kh='GTGT rieng')
			    ;
---3		luot (neu khong co yeu cau)
    -- Dot 1:    dot 2 khong chay
		update ttkd_bsc.ct_bsc_ptm a 
			set thang_tldg_dt=202404, lydo_khongtinh_dongia=null
		    -- select ma_tb, doituong_id,  lydo_khongtinh_luong, nocuoc_ptm, trangthaitb_id, dichvuvt_id, loaitb_id, thoihan_id, mien_hsgoc, nop_du, lydo_khongtinh_dongia, thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a  
		    where thang_ptm=202404 and (loaitb_id not in (20, 21,131, 358) or ma_kh='GTGT rieng')  and (chuquan_id is null or chuquan_id !='264')
						  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)						  
						  and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
							)
							and thang_tldg_dt is null 
						    and (dthu_ps > 0 
									or (nvl(dthu_ps, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc > 202404)
							) 
						and nocuoc_ptm is null 		
						  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
						  and  (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )            
						  and (nop_du=1 or mien_hsgoc is not null) 
						  and (loai_ld not like '_LCN' or loai_ld is null)
						  and manv_ptm is not null
						  and not exists (select 1 from ttkd_bct.ptm_codinh_202404
												where ma_pb='VNP0703000' and nguoi_cn_goc in ('freedoo','myvnpt','shop.vnpt.vn','dhtt.mytv') and thuebao_id=a.thuebao_id )
				;
---4  dot 2 khong chay


    -- Dot 2:    
		update ttkd_bsc.ct_bsc_ptm a 
		    set thang_tldg_dt=202404
		    -- select thang_ptm, ma_gd, ma_tb, manv_ptm, loai_ld, ma_pb, lydo_khongtinh_luong, lydo_khongtinh_dongia, dthu_goi, dthu_ps, nocuoc_ptm, ten_pb, trangthaitb_id, loaitb_id, thoihan_id, ngay_luuhs_ttkd, ngay_luuhs_ttvt, nop_du, mien_hsgoc, thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a  
				 where thang_ptm=202404 and (loaitb_id not in (210, 21,131) or ma_kh='GTGT rieng') and (chuquan_id is null or chuquan_id<>264) 
				  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id) 
				 -- and lydo_khongtinh_luong is null 
				 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
							)
				  and thang_tldg_dt is null 
				--  and a.ma_tb in ('hcm_hddt_mtt_00000417')
				  and (dthu_ps > 0 
									or (nvl(dthu_ps, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc > 202404)
							) 
				and nocuoc_ptm is null 
				  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
				  and  (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )            
				  and (nop_du=1 or mien_hsgoc is not null) 
				  and (loai_ld not like '_LCN' or loai_ld is null)	---khong tinh dai ly ca nhan
				  and manv_ptm is not null
				  
				  ;
 ---5 
 
    -- ACT
--		update ttkd_bsc.ct_bsc_ptm a set thang_tldg_dt=null
--		    where chuquan_id=264 and lydo_khongtinh_luong is null and thang_tldg_dt=202404
--		    ;
         --6

		update ttkd_bsc.ct_bsc_ptm a 
			   set thang_tldg_dt=202404, thang_tlkpi=202404, thang_tlkpi_to=202404, thang_tlkpi_phong=202404
				    ,thang_tldg_dnhm=case when tien_dnhm>0 and ngay_tt is not null then 202404 end
				    ,thang_tlkpi_dnhm=case when tien_dnhm>0 and ngay_tt is not null then 202404 end
		    -- select * from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm>=202401 and chuquan_id=264 and manv_ptm is not null
			   and lydo_khongtinh_luong is null and thang_xacnhan_khkt is not null 
			   and thang_tldg_dt is null 
			   ; 
		---7

-- VNPTS cua BHOL tiep thi:
--		update ttkd_bsc.ct_bsc_ptm set thang_tldg_dt_dai=null
--		    where thang_ptm=202404 and loaitb_id<>21 and manv_tt_dai is not null and thang_tldg_dt_dai is not null
--		    ;
		--8
		update ttkd_bsc.ct_bsc_ptm a 
			set thang_tldg_dt_dai=202404
		    -- select * from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm=202404 and loaitb_id<>21
				  and lydo_khongtinh_luong is null and thang_tldg_dt_dai is null         
				  and dthu_ps is not null and nocuoc_ptm is null                       
				  and (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
				  and (nop_du=1 or mien_hsgoc is not null)      
				  and not exists (select 1 from ttkd_bsc.nhanvien_202404 where loai_ld like '_LCN' and manv_hrm=a.manv_tt_dai) 
				  and manv_tt_dai is not null  
				  
				  ;
		----9

-- PGP
--		update ttkd_bsc.ct_bsc_ptm set thang_tldg_dt_nvhotro=null
--		    where thang_ptm=202404 and (loaitb_id not in (21,131) or ma_kh='GTGT rieng')
--					  and manv_hotro is not null and thang_tldg_dt_nvhotro is not null
--					  ;
		--10
    
    
		update ttkd_bsc.ct_bsc_ptm a 
			   set thang_tldg_dt_nvhotro=202404
		    -- select ma_tb, thang_tldg_dt_nvhotro, lydo_khongtinh_luong  from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm=202404 and (loaitb_id not in (21,131) or ma_kh='GTGT rieng')
				  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
				  and (lydo_khongtinh_luong is null or lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
				  and thang_tldg_dt_nvhotro is null         
				  and (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
				  and dthu_ps is not null and nocuoc_ptm is null                    
				  and exists(select 1 from ttkd_bsc.nhanvien_202404 where ma_pb='VNP0702600' and ma_nv=a.manv_hotro)
				  
				;
		--11

   -- dnhm:  
--			update ttkd_bsc.ct_bsc_ptm a set thang_tldg_dnhm=null
--			    where thang_ptm = 202404 and thang_tldg_dnhm = 202404
			    ;
			    --12
        
		update ttkd_bsc.ct_bsc_ptm a 
			   set thang_tldg_dnhm=202404
		    -- select chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt, thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm,  to_char(ngay_tt,'yyyymm'), ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm=202404 and (loaitb_id not in (20,21,210, 222) or ma_kh='GTGT rieng')   
				  and thang_tldg_dnhm is null 
				  and thang_tldg_dt=202404             
				  -- and lydo_khongtinh_luong is null 
						 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
								)
				  and chuquan_id in (145,266,264)
				  and (tien_dnhm>0 or tien_sodep>0) and ngay_tt is not null and trangthai_tt_id=1
				  and (loai_ld not like '_LCN' or loai_ld is null)         
				  and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
				  
				  ;
				  --13
                
 
 -- dnhm vnpts: 
			 update ttkd_bsc.ct_bsc_ptm a 
				   set thang_tldg_dnhm=thang_ptm
	-- select chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt, thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm,  to_char(ngay_tt,'yyyymm'), ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a

			    where thang_ptm=202404 and loaitb_id=20
				   and thang_tldg_dnhm is null and thang_tldg_dt=202404
				   and lydo_khongtinh_luong is null
				   and (tien_dnhm>0 or tien_sodep>0) 
				   and (loai_ld not like '_LCN' or loai_ld is null)
			;
               --14     
commit;
--***************** ky n-1 ******************
--			update ttkd_bsc.ct_bsc_ptm a 
--			    set thang_tldg_dt=''
--			 where thang_ptm=202403 ----thang n-1
--						and (loaitb_id<>21 or ma_kh='GTGT rieng') and thang_tldg_dt=202404		--thang n
--			 ; 
		--15

-- Dot 1: dot 2 khong chay
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt=202404		--thang n
			    -- select ma_gd, ma_tb, dthu_ps,dthu_ps_n1, trangthaitb_id_n1  from ttkd_bsc.ct_bsc_ptm a 
			    where thang_ptm=202403		--thang n-1
						and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') and chuquan_id in (145,266) 
					  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
					  and thang_tldg_dt is null and lydo_khongtinh_luong is null
					  and ( trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )         
					  and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 
									or (nvl(dthu_ps_n1, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc > 202404)
							) 
					and nocuoc_n1 is null 
					  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
					  and (nop_du=1 or mien_hsgoc is not null) 
					  and (loai_ld not like '_LCN' or loai_ld is null)
					  ;
			--16  dot 2 khong chay
			commit;
			
-- dnhm 
--			update ttkd_bsc.ct_bsc_ptm a set thang_tldg_dnhm=null
--			    where thang_ptm=202403 ---thang n-1
--							and thang_tldg_dnhm=202404			--thang n
--			    ;
			    --17
        
		update ttkd_bsc.ct_bsc_ptm a 
					set thang_tldg_dnhm=202404		--thang n
		    -- select chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt,  thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm,  to_char(ngay_tt,'yyyymm'), ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm=202403 ---thang n-1
					and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng')  and chuquan_id in (145,266,264) 
					 and thang_tldg_dnhm is null 
					 and thang_tldg_dt = 202404   --thang n          
					 -- and lydo_khongtinh_luong is null 
						 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
								)
					 
					 and (tien_dnhm>0 or tien_sodep>0)
					 and ((loaitb_id not in (20,149)  and ngay_tt is not null and trangthai_tt_id=1 ) or loaitb_id in (20,149))
					 and (loai_ld not like '_LCN' or loai_ld is null)         
					 and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
					 ;
				--18

                      
-- Dot 2:
--			update ttkd_bsc.ct_bsc_ptm a 
--			    set thang_tldg_dt='' 
--			 where thang_ptm =202403 ---thang n-1
--						and thang_tldg_dt=202404 ---thang n
--			 ;
			--19
 
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt=202404 ---thang n
			-- select ma_tb, dich_vu, lydo_khongtinh_luong, lydo_khongtinh_dongia, manv_ptm, loai_ld, tien_dnhm, thang_tldg_dnhm from ttkd_bsc.ct_bsc_ptm a 
			 where thang_ptm=202403 ---thang n-1
					and (chuquan_id in (145,266) or chuquan_id is null) and thang_tldg_dt is null 
				   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
				   and (loaitb_id<>21 or ma_kh='GTGT rieng')
				 --  and lydo_khongtinh_luong is null 
				  and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
							)
							
				   and ( trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
				
				     and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 
									or (nvl(dthu_ps_n1, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc > 202404)
							) 
					and nocuoc_n1 is null 
				   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149)) ---VNPts khong xet trang thai TT
				   and (nop_du=1 or mien_hsgoc is not null) 
				   and (loai_ld not like '_LCN' or loai_ld is null)
				
			
				   ;
				   --20
     

			update ttkd_bsc.ct_bsc_ptm a 
				set thang_tldg_dnhm=202404 ---thang n
			    -- select lydo_khongtinh_dongia, chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt,  thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm,  to_char(ngay_tt,'yyyymm'), ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202403 ---thang n-1
						and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng')   
						 and thang_tldg_dnhm is null and thang_tldg_dt=202404 --- thang n           
						 -- and lydo_khongtinh_luong is null 
						 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
								)
						  
						 and chuquan_id in (145,266,264)     
						 and (tien_dnhm>0 or tien_sodep>0)
						 and ((loaitb_id not in (20,149)  and ngay_tt is not null and trangthai_tt_id=1 ) or loaitb_id in (20,149))
						 and (loai_ld not like '_LCN' or loai_ld is null)         
						 and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
						 
				;
						 ---21
                
                            
-- VNPTS cua BHOL:
--				update ttkd_bsc.ct_bsc_ptm a 
--				    set thang_tldg_dt_dai=''
--				    where thang_ptm=202403  ---thang n-1
--								and thang_tldg_dt_dai=202404 ---thang n
--								and manv_tt_dai is not null and loaitb_id<>21 
--				;
				--22
      
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt_dai =202404		---thang n
			-- select * from ttkd_bsc.ct_bsc_ptm a 
			   where thang_ptm=202403 	---thang n-1
					and manv_tt_dai is not null and loaitb_id<>21 
				   and (loaitb_id<>21 or ma_kh='GTGT rieng')
				   and lydo_khongtinh_luong is null and thang_tldg_dt_dai is null  
				   and thang_tldg_dt = 202404		---thang n
				   and not exists (select 1 from ttkd_bsc.nhanvien_202404 where loai_ld like '_LCN' and manv_hrm=a.manv_tt_dai)
				   
				   ;
			--23

-- PGP
--			update ttkd_bsc.ct_bsc_ptm 
--					set thang_tldg_dt_nvhotro=null
--					    where thang_ptm=202403	--thang n-1
--								 and thang_tldg_dt_nvhotro=202404		--thang n
--								and (loaitb_id not in (21,131) or ma_kh='GTGT rieng')
--							  and manv_hotro is not null
--						;
				--24
        
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt_nvhotro=202404			--thang n
			    -- select thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202403 		--thang n-1
					and (loaitb_id not in (21) or ma_kh='GTGT rieng') 
					  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
					  and lydo_khongtinh_luong is null and thang_tldg_dt_nvhotro is null         
					  and ( trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
					  and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 and nocuoc_n1 is null      
					  and ((loaitb_id not in (20,149) and trangthai_tt_id=1 ) or loaitb_id in (20,149))
					  and exists(select 1 from ttkd_bsc.nhanvien_202404 where ma_pb='VNP0702600' and ma_nv=a.manv_hotro)
					
				;
			--25
			commit;
-- ******************ky n-2 ****************
--			update ttkd_bsc.ct_bsc_ptm a 
--			    set thang_tldg_dt=''
--			 where thang_ptm=202402 ---thang n-2
--						and thang_tldg_dt=202404 ---thang n
--						and (loaitb_id<>21 or ma_kh='GTGT rieng')
			 ;
			--26
 
 -- Dot 1:  dot 2 khong chay
			 update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt=202404		---thang n
			    -- select * from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202402 		--thang n-2
						and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') and chuquan_id in (145,266) 
					  and (loaitb_id<>21 or ma_kh='GTGT rieng') 
					  and lydo_khongtinh_luong is null and thang_tldg_dt is null  
					  and ( trangthaitb_id_n2=1 or (loaitb_id=20 and trangthaitb_id_n2=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
					   and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)>0 
									or (nvl(dthu_ps_n2, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc > 202404)
							) 
					and nocuoc_n2 is null  
					  
					  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
					  and (nop_du=1 or mien_hsgoc is not null) 
					  and (loai_ld not like '_LCN' or loai_ld is null)
				;
				--27  dot 2 khong chay

--				update ttkd_bsc.ct_bsc_ptm a set thang_tldg_dnhm=null
--				    where thang_ptm=202402		--thang n-2
--								and thang_tldg_dnhm=202404		--thang n
				    ;
				--28
				
			update ttkd_bsc.ct_bsc_ptm a 
					set thang_tldg_dnhm=202404	--thang n
			    -- select chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt,  thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm,  to_char(ngay_tt,'yyyymm'), ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202402 ---thang n-2
							and thang_tldg_dt=202404		---thang n
						and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng')   
						 and thang_tldg_dnhm is null              
						 -- and lydo_khongtinh_luong is null 
						 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
								)
						 and chuquan_id in (145,266,264)     
						 and (tien_dnhm>0 or tien_sodep>0)
						 and ((loaitb_id not in (20,149)  and ngay_tt is not null and trangthai_tt_id=1 ) or loaitb_id in (20,149))
						 and (loai_ld not like '_LCN' or loai_ld is null)         
						 and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
			 ;
			 ---END dot 1
			 ---29
                
                   
 -- Dot 2:
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt=202404		---thang n
			    -- select chuquan_id, ma_tb, ma_pb, loai_ld, thoihan_id, manv_ptm, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202402 	---thang n-2
					and thang_tldg_dt is null 
					  and (loaitb_id<>21 or ma_kh='GTGT rieng') and (chuquan_id in  (145,266) or chuquan_id is null)
					  and lydo_khongtinh_luong is null 
					  and ( trangthaitb_id_n2=1 or (loaitb_id=20 and trangthaitb_id_n2=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
					  
					  and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)>0 
									or (nvl(dthu_ps_n2, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc > 202404)
							) 
					and nocuoc_n2 is null  
					  
					  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
					  and (nop_du=1 or mien_hsgoc is not null) 
					  and (loai_ld not like '_LCN' or loai_ld is null)
				;
				--30


--			update ttkd_bsc.ct_bsc_ptm a 
--			    set thang_tldg_dnhm=null
--			    where thang_ptm=202402 	---thang n-2
--							and thang_tldg_dnhm=202404				---thang n
			;
			--31
        
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dnhm =202404	---thang n
			    -- select chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt,  thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm,  to_char(ngay_tt,'yyyymm'), ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm =202402 	---thang n-2
						and thang_tldg_dt=202404 	---thang n
					and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng')   
					  and thang_tldg_dnhm is null             
					  -- and lydo_khongtinh_luong is null 
						 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
								)
					  and chuquan_id in (145,266,264)     
					  and (tien_dnhm>0 or tien_sodep>0)
					  and ((loaitb_id not in (20,149)  and ngay_tt is not null and trangthai_tt_id=1 ) or loaitb_id in (20,149))
					  and (loai_ld not like '_LCN' or loai_ld is null)         
					  and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
			;
			--32
                  
     
-- VNPTS cua BHOL:
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt_dai =202404		--thang n
			 -- select * from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202402		--thang n-2
					and thang_tldg_dt=202404	--thang n
					and thang_tldg_dt_dai is null 
				   and (loaitb_id<>21 or ma_kh='GTGT rieng')
				   and lydo_khongtinh_luong is null and thang_tldg_dt_dai is null  
				   and manv_tt_dai is not null and loaitb_id=20
				   and not exists (select 1 from ttkd_bsc.nhanvien_202404 where loai_ld like '_LCN' and manv_hrm=a.manv_tt_dai)
			;
			--33

-- PGP
--			update ttkd_bsc.ct_bsc_ptm 
--			    set thang_tldg_dt_nvhotro=null
--			    where thang_ptm =202402 		---thang n-2
--						and thang_tldg_dt_nvhotro=202404	---thang n
--						and (loaitb_id not in (21,131) or ma_kh='GTGT rieng')
--						  and manv_hotro is not null 
				;
		--34
    
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt_nvhotro=202404		---thang n
			    -- select thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202402 --thang n-2
					and (loaitb_id not in (21,131) or ma_kh='GTGT rieng')
					  and lydo_khongtinh_luong is null and thang_tldg_dt_nvhotro is null         
					  and ( trangthaitb_id_n2=1 or (loaitb_id=20 and trangthaitb_id_n2=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
					  and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)>0 and nocuoc_n2 is null  
					  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
					  and exists(select 1 from ttkd_bsc.nhanvien_202404 where ma_pb='VNP0702600' and ma_nv=a.manv_hotro)
					  
			;
            ---35
commit;
--*************** ky n-3 *********
--			update ttkd_bsc.ct_bsc_ptm a 
--			    set thang_tldg_dt=''
--			    where thang_ptm=202401 ---thang n-3
--							and thang_tldg_dt=202404	--thang n
--							and (loaitb_id<>21 or ma_kh='GTGT rieng')
			    ;
		--36

-- Dot 1: dot 2 khong chay
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt=202404			---thang n
			   -- select thang_ptm, dich_vu, ma_tb, ma_to, loai_ld, trangthaitb_id_n3, nocuoc_n3, ngay_luuhs_ttkd,nop_du, thang_tldg_dt, heso_hoso from ttkd_bsc.ct_bsc_ptm a 
			    where thang_ptm=202401 ---thang n-3
					and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') and chuquan_id in (145,266) 
					  and lydo_khongtinh_luong is null and thang_tldg_dt is null 
					  and ( trangthaitb_id_n3=1 or (loaitb_id=20 and trangthaitb_id_n3=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)))or loaitb_id in (89,90,146) )
				
					    and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)>0 
									or (nvl(dthu_ps_n2, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc > 202404)
							) 
					and nocuoc_n3 is null
					  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
					  and (nop_du=1 or mien_hsgoc is not null) 
					  and (loai_ld not like '_LCN' or loai_ld is null)
			;
		--37 dot 2 khong chay
		
--			update ttkd_bsc.ct_bsc_ptm a set thang_tldg_dnhm=null
--			    where thang_ptm=202401 ---thang n-3
--							and thang_tldg_dnhm=202404		---thang n
			    ;
        --38
	   
			update ttkd_bsc.ct_bsc_ptm a 
				set thang_tldg_dnhm=202404	--thang n
			    -- select chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt,  thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm,  to_char(ngay_tt,'yyyymm'), ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202401	--thang n-3
						and thang_tldg_dt=202404		---thang n
						and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng')   
						 and thang_tldg_dnhm is null              
						-- and lydo_khongtinh_luong is null 
						 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
								)
						 and chuquan_id in (145,266,264)     
						 and (tien_dnhm>0 or tien_sodep>0)
						 and ((loaitb_id not in (20,149)  and ngay_tt is not null and trangthai_tt_id=1 ) or loaitb_id in (20,149))
						 and (loai_ld not like '_LCN' or loai_ld is null)         
						 and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
						 ;
                --39               

-- Dot 2: 
				update ttkd_bsc.ct_bsc_ptm a 
					   set thang_tldg_dt=202404		--thang n
				   -- select chuquan_id, thang_ptm, dich_vu, ma_pb, ma_tb, ma_to, loai_ld, trangthaitb_id_n3, nocuoc_n3, dthu_ps, dthu_ps_n1,dthu_ps_n2, dthu_ps_n3, ngay_luuhs_ttkd,nop_du, thang_tldg_dt, heso_hoso, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a 
				    where thang_ptm=202401 		---thang n-3
						  and (loaitb_id<>21 or ma_kh='GTGT rieng') and chuquan_id in (145,266)
						  --and lydo_khongtinh_luong is null 
						   and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
							)
						  and thang_tldg_dt is null 
						  and (trangthaitb_id_n3=1 or (loaitb_id=20 and trangthaitb_id_n3=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)))or loaitb_id in (89,90,146) )
												  
						    and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)>0 
									or (nvl(dthu_ps_n2, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc > 202404)
							) 
							and nocuoc_n3 is null
					
						  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
						  and (nop_du=1 or mien_hsgoc is not null) 
						  and (loai_ld not like '_LCN' or loai_ld is null)
					;
		--40

				update ttkd_bsc.ct_bsc_ptm a 
						set thang_tldg_dnhm=202404		---thang n
				    -- select chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt,  thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm,  to_char(ngay_tt,'yyyymm'), ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm=202401 		---thang n-3
							and thang_tldg_dt=202404      	---thang n
							and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng')   
							 and thang_tldg_dnhm is null        
							 -- and lydo_khongtinh_luong is null 
							and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
								)
							 and chuquan_id in (145,266,264)     
							 and (tien_dnhm>0 or tien_sodep>0)
							 and ((loaitb_id not in (20,149)  and ngay_tt is not null and trangthai_tt_id=1 ) or loaitb_id in (20,149))
							 and (loai_ld not like '_LCN' or loai_ld is null)         
							 and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
				;


-- VNPTS cua BHOL
				update ttkd_bsc.ct_bsc_ptm a 
					   set thang_tldg_dt_dai=202404		---thang n
				   -- select * from ttkd_bsc.ct_bsc_ptm a 
				    where thang_ptm=202401 ---thang n-3
								and thang_tldg_dt_dai is null 
							   and (loaitb_id<>21 or ma_kh='GTGT rieng')
							   and lydo_khongtinh_luong is null     
							   and ( trangthaitb_id_n3=1 or (loaitb_id=20 and trangthaitb_id_n3=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)))or loaitb_id in (89,90,146) )
							   and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)>0 and nocuoc_n3 is null
							   and (nop_du=1 or mien_hsgoc is not null) 
							   and manv_tt_dai is not null
							   and not exists (select 1 from ttkd_bsc.nhanvien_202404 where loai_ld like '_LCN' and manv_hrm=a.manv_tt_dai)
				;


-- PGP
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt_nvhotro=202404		---thang n
			    -- select thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm=202401 	---thang n-3
						and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') and chuquan_id in (145,266) 
					--  and lydo_khongtinh_luong is null 
					 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
							)
					  and thang_tldg_dt_nvhotro is null         
					  and ( trangthaitb_id_n3=1 or (loaitb_id=20 and trangthaitb_id_n3=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
					  and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)>0 and nocuoc_n3 is null
					  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
					  and exists(select 1 from ttkd_bsc.nhanvien_202404 where ma_pb='VNP0702600' and ma_nv=a.manv_hotro)
			;
            
 commit; 
-- GTGT rieng thang n
			update ttkd_bsc.ct_bsc_ptm a 
				   set  thang_tldg_dt=202404, thang_tlkpi=202404, thang_tlkpi_to=202404, thang_tlkpi_phong=202404,
						 thang_tldg_dt_nvhotro=case when manv_hotro is not null and thang_tldg_dt_nvhotro is null then 202404 end
			    -- select * from ttkd_bsc.ct_bsc_ptm a 
			    where thang_ptm>=202401 ---thang n-3
								and lydo_khongtinh_luong is null and thang_xacnhan_khkt is not null 
							   and thang_tldg_dt is null and ma_kh='GTGT rieng'
			;
   
        
-- Tinh bs ho so gia han CA, IVAN,... dthu ps =0 nhung chua den ky dat coc moi: 
	---khong chay thang 5/2025, KIEMTRA xem da duoc tinh khong?
			--update ttkd_bsc.ct_bsc_ptm a 
				   set thang_tldg_dt=202404, thang_tlkpi=202404, thang_tlkpi_to=202404, thang_tlkpi_phong=202404,
						thang_tldg_dt_nvhotro=case when manv_hotro is not null and thang_tldg_dt_nvhotro is null then 202404 end
						--, thang_luong = 87
			    -- select * from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm>=202401 		---thang n-3
						and thang_bddc>202404			---thang n
					--and lydo_khongtinh_luong is null 
					 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' and heso_daily=0.05 and MA_NGUOIGT is not null)
							)
					and thang_tldg_dt is null
				   and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
			--	   and ma_tb = 'hcm_ca_00067906'
			;
			rollback;
			commit;
                
---- PGP - thieu hsg thang n
--				update ttkd_bsc.ct_bsc_ptm a 
--				    set  thang_tldg_dt_nvhotro=202404, thang_tlkpi_hotro=202404
--				    -- select * from ttkd_bsc.ct_bsc_ptm_pgp
--				    where thang_ptm>=202401		---thang n-3
--					   and lydo_khongtinh_dongia='; Chua nop du ho so'
--					   and thang_tldg_dt_nvhotro is null
--					   ;
--        
        
---- SMS Brn dot 1 ngay 5: khong chay dot 2 do chua co cuoc ps thang n+1
--				update ttkd_bsc.ct_bsc_ptm a 
--				    set thang_tldg_dt='', thang_tlkpi='', thang_tlkpi_to='', thang_tlkpi_phong=''
--			--- select * from ttkd_bsc.ct_bsc_ptm a 
--				    where loaitb_id in(131,358) 
--								and thang_ptm=202403		---thang n-1
--								and thang_tldg_dt=202404 		---thang n
--				;
--
commit;
  ----kiem tien dau noi hoa mang---
select thang_ptm, ma_gd, ma_tb, soseri, ngay_tt, tien_tt, tien_dnhm, trangthai_tt_id, chuquan_id, manv_ptm, MA_NGUOIGT
			, lydo_khongtinh_luong, heso_daily
  from ttkd_bsc.ct_bsc_ptm a
  where thang_tldg_dt=202404 and trangthai_tt_id=1 and (tien_dnhm>0 or tien_sodep >0) and ngay_tt is not null
    and exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id)
    and thang_tldg_dnhm is null
    ;
    ----Kiem tra lydo_khongtinh_XXX co gia tri
    select thang_ptm, ma_gd, ma_tb, soseri, ngay_tt, tien_tt, tien_dnhm, trangthai_tt_id, chuquan_id, manv_ptm, MA_NGUOIGT
			, lydo_khongtinh_luong, lydo_khongtinh_dongia, heso_daily
  from ttkd_bsc.ct_bsc_ptm a
  where thang_tldg_dt=202404 
					and (lydo_khongtinh_luong is not null or lydo_khongtinh_dongia is not null )
	;
	
