		
commit;
rollback;
-- ID447: cap nhat ktoan kh xac nhan thu tien
	update ttkd_bsc.ct_bsc_ptm a 
		   set (thang_luong, xacnhan_khkt, thang_xacnhan_khkt, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, thang_tldg_dt_nvhotro, thang_tlkpi_hotro) =       
				 (select 7, tienthu_khkt, to_char(ngaycapnhat_khkt,'yyyymm') , 202409, 202409, 202409, 202409, 202409, 202409
					from ttkdhcm_ktnv.amas_duan_ngoai_doanhthu where id=a.id_447)
--	     select * from ttkd_bsc.ct_bsc_ptm a
	    where  thang_ptm >= 202406 and thang_xacnhan_khkt is null ---thang n-3
					and a.id_447 is not null
					and nvl(thang_tldg_dt, 202409) = 202409
			and exists(select * from ttkdhcm_ktnv.amas_duan_ngoai_doanhthu where ngaythanhtoan_khkt is not null and id=a.id_447)
			;

       ---2  dot 1, 2
-- ******************Ky n ***************** 
		
---3		luot (neu khong co yeu cau)
   
---4  dot 2 khong chay

create index ttkd_bsc.idx_thangptm on ttkd_bsc.ct_bsc_ptm (thang_ptm);

    -- Dot 2:    
		update ttkd_bsc.ct_bsc_ptm a 
		    set thang_tldg_dt = a.thang_ptm
--		    select thang_luong, thang_ptm, ma_gd, thuebao_id, ma_tb, manv_ptm, loai_ld, ma_pb, lydo_khongtinh_luong, lydo_khongtinh_dongia, luong_dongia_nvptm, dthu_goi, dthu_ps, nocuoc_ptm, ten_pb, trangthaitb_id, loaitb_id, thoihan_id, ngay_luuhs_ttkd, ngay_luuhs_ttvt, nop_du, mien_hsgoc, thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a  
				 where a.thang_ptm = 202409
--							and thang_luong = 86
							and (loaitb_id not in (210, 21,131) or ma_kh='GTGT rieng') and (chuquan_id is null or chuquan_id<>264) 
				  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id) 
				 -- and lydo_khongtinh_luong is null 
				 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' )
							)
				  and nvl(thang_tldg_dt, 999999)  >= a.thang_ptm 
--				  and thang_tldg_dt is null
--				  and a.ma_tb in ('hcm_econtract_00000999')
				  and (dthu_ps > 0 
									or (nvl(dthu_ps, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc >= a.thang_ptm
										)
							) 
				and nocuoc_ptm is null 
				  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
				  and  (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )            
				  and (nop_du=1 or mien_hsgoc is not null) 
				  and (loai_ld not like '_LCN' or loai_ld is null)	---khong tinh dai ly ca nhan dongia (aNguyen dang tinh), nguoc lai tinh KPI
				 
				  ;
 ---5 
 
    -- ACT

         --6

		update ttkd_bsc.ct_bsc_ptm a 
			   set thang_tldg_dt = 202409, thang_tlkpi=202409, thang_tlkpi_to=202409, thang_tlkpi_phong=202409
				    ,thang_tldg_dnhm = case when tien_dnhm>0 and ngay_tt is not null then 202409 end
				    ,thang_tlkpi_dnhm = case when tien_dnhm>0 and ngay_tt is not null then 202409 end
				    , thang_tlkpi_dnhm_to = case when tien_dnhm>0 and ngay_tt is not null then 202409 end
				    , thang_tlkpi_dnhm_phong = case when tien_dnhm>0 and ngay_tt is not null then 202409 end
--		     select  thang_ptm, ma_gd, thuebao_id, ma_tb, manv_ptm, loai_ld, ma_pb, lydo_khongtinh_luong, lydo_khongtinh_dongia, luong_dongia_nvptm
--					, dthu_goi, dthu_ps, nocuoc_ptm, ten_pb, trangthaitb_id, loaitb_id, thoihan_id, ngay_luuhs_ttkd, ngay_luuhs_ttvt, nop_du, mien_hsgoc
--					, thang_xacnhan_khkt, thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong, thang_tldg_dnhm, thang_tlkpi_dnhm, thang_tlkpi_dnhm_to, thang_tlkpi_dnhm_phong    from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm >= 202406 and chuquan_id=264 --and manv_ptm is not null
			   and lydo_khongtinh_luong is null and thang_xacnhan_khkt is not null 
			   and nvl(thang_tldg_dt, 999999)  >= 202409
			   
			   ; 
		---7

-- PGP or NVIEN ban DIGISHOP
--	
		--10
    commit;
    
		update ttkd_bsc.ct_bsc_ptm a 
			   set thang_tldg_dt_nvhotro = a.thang_ptm
					, thang_tldg_dt_dai = a.thang_ptm
--		    select  thang_luong, ma_tb, manv_ptm, manv_hotro, thang_tldg_dt, thang_tldg_dt_nvhotro, thang_tldg_dt_dai, luong_dongia_nvptm, lydo_khongtinh_luong, lydo_khongtinh_dongia, nguon, ma_gd_gt, tyle_hotro, nop_du, mien_hsgoc, heso_daily  from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm = 202409 --and ma_tb = 'hcm_econtract_00000999'
								and (loaitb_id not in (210, 21,131) or ma_kh='GTGT rieng') and (chuquan_id is null or chuquan_id<>264) 
							  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id) 
							and ( (thang_tldg_dt is null and lydo_khongtinh_luong like '%Phat trien qua Dai ly%')			---PGP dc tinh khi ban qua dai ly
										or thang_tldg_dt is not null
									)
							  and nvl(thang_tldg_dt_nvhotro, 999999)  >= a.thang_ptm 

							  and (dthu_ps > 0 
												or (nvl(dthu_ps, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
																and dthu_goi >0 and thang_bddc > a.thang_ptm
													)
										) 
							and nocuoc_ptm is null 
							  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
							  and  (trangthaitb_id=1 or (loaitb_id=20 and trangthaitb_id=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )            
							  and (nop_du=1 or mien_hsgoc is not null) 
--							  and manv_hotro is not null
--							  and thang_luong = 26
--				and vanban_id = 764  --onlt T7, T8, T9

				;
			
		--11
   -- dnhm:  
--			
			    ;
			    --12
        
		update ttkd_bsc.ct_bsc_ptm a 
			   set thang_tldg_dnhm = a.thang_ptm
--		     select chuquan_id, ma_tb, manv_ptm, ten_pb, dich_vu, thang_tldg_dt, thang_tldg_dnhm, tien_dnhm, thang_ptm, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a
		    where thang_ptm = 202409
							and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng')   
						  and nvl(thang_tldg_dnhm, 999999) >= 202409		 ---thang n

								 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' )
										)
						  and chuquan_id in (145, 266, 264)
						  and (tien_dnhm>0 or tien_sodep>0)
						  and nocuoc_ptm is null 
						  and ((loaitb_id not in (20,149, 21)  and trangthai_tt_id=1 ) or loaitb_id in (20,149))
						  and (nop_du = 1 or mien_hsgoc is not null)
						  and (loai_ld not like '_LCN' or loai_ld is null)         
						  and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
				
				  ;
				  --13
commit;
--***************** ky n-1 ******************
--			
		--15
                      
-- Dot 2:
--			
--			 ;
			--19
 
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt = 202409 ---thang n
--			select ma_tb, dich_vu, lydo_khongtinh_luong, lydo_khongtinh_dongia, manv_ptm, loai_ld, tien_dnhm, luong_dongia_nvptm,  thang_tldg_dnhm,  thang_tldg_dt from ttkd_bsc.ct_bsc_ptm a 
			 where thang_ptm = 202408 ---thang n-1
					and (chuquan_id in (145,266) or chuquan_id is null) 

					and nvl(thang_tldg_dt, 999999) >= 202409
				   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
				   and (loaitb_id<>21 or ma_kh='GTGT rieng')
				  and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
							)
				   and ( trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
				
				     and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 
									or (nvl(dthu_ps_n1, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
													and dthu_goi >0 and thang_bddc > a.thang_ptm		---thang n
										)
							) 
					and nocuoc_n1 is null 
				   and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149)) ---VNPts khong xet trang thai TT
				   and (nop_du=1 or mien_hsgoc is not null) 
				   and (loai_ld not like '_LCN' or loai_ld is null)
			
--					and manv_ptm is not null
--			and thang_luong = 17
				   ;
				   --20
     

			update ttkd_bsc.ct_bsc_ptm a 
				set thang_tldg_dnhm = 202409 ---thang n
--			     select lydo_khongtinh_dongia, chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt,  thang_tldg_dnhm, tien_dnhm, tien_tt, heso_dichvu_dnhm, thang_ptm, LUONG_DONGIA_DNHM_NVPTM, ngay_tt, soseri, trangthai_tt_id from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202408 ---thang n-1
						and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng')   
						and nvl(thang_tldg_dnhm, 999999) >= 202409		 ---thang n
						 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
								)
						 and chuquan_id in (145,266,264)     
						 and (tien_dnhm>0 or tien_sodep>0)
						 and ((loaitb_id not in (20,149, 21)  and trangthai_tt_id=1 ) or loaitb_id in (20,149))
						  and nocuoc_n1 is null 
						  and (nop_du = 1 or mien_hsgoc is not null)
						 and (loai_ld not like '_LCN' or loai_ld is null)         
						 and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
						
				;
						 ---21
                

-- PGP
--		
--						;
				--24
        
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt_nvhotro = 202409		--thang n
--			    select ma_tb, thang_tldg_dt, thang_tldg_dt_nvhotro, heso_daily, lydo_khongtinh_dongia, luong_dongia_nvptm, luong_dongia_nvhotro, manv_hotro from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202408		--thang n-1
							and nvl(thang_tldg_dt_nvhotro, 999999) >= 202409		---thang n
							and (loaitb_id not in (210, 21,131) or ma_kh='GTGT rieng') and (chuquan_id is null or chuquan_id<>264)
							  and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
								
								and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%')		---PGP dc tinh khi ban qua dai ly
												)
							and ( (thang_tldg_dt is null and lydo_khongtinh_luong like '%Phat trien qua Dai ly%')			---PGP dc tinh khi ban qua dai ly
										or thang_tldg_dt is not null				---tam thoi PTM < T5, sau do chuyen ve is not null sau >= T5
									)
							  
							  and ( trangthaitb_id_n1=1 or (loaitb_id=20 and trangthaitb_id_n1=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
							  and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)>0 and nocuoc_n1 is null      
							  and ((loaitb_id not in (20,149) and trangthai_tt_id=1 ) or loaitb_id in (20,149))
							  and (nop_du = 1 or mien_hsgoc is not null) 
							
				;
			--25
			commit;
-- ******************ky n-2 ****************
--		
			 ;
			--26
     
 -- Dot 2:
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt = 202409		---thang n
--			     select thang_luong, ma_tb, ma_pb, loai_ld, thoihan_id, manv_ptm, thang_tldg_dt, luong_dongia_nvptm, lydo_khongtinh_luong, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202407 	---thang n-2
							and nvl(thang_tldg_dt, 999999) >= 202409  ---thang n
							and (loaitb_id<>21 or ma_kh='GTGT rieng') and (chuquan_id in  (145,266) or chuquan_id is null)
						   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
						  and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
								)
							  and ( trangthaitb_id_n2=1 or (loaitb_id=20 and trangthaitb_id_n2=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
							  and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)>0 
											or (nvl(dthu_ps_n2, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
															and dthu_goi >0 and thang_bddc > a.thang_ptm        	
													)
									) 
							and nocuoc_n2 is null  
							  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
							  and (nop_du=1 or mien_hsgoc is not null) 
							  and (loai_ld not like '_LCN' or loai_ld is null)
							  
				;
				--30

			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dnhm = 202409	---thang n
--			     select chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt,  thang_tldg_dnhm, tien_dnhm, tien_tt, thang_ptm, ngay_tt, trangthai_tt_id, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202407 	---thang n-2
					and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng') and chuquan_id in (145,266,264)
					  and nvl(thang_tldg_dnhm, 999999) >= 202409
				
						 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' )
								)
					  and nocuoc_n2 is null
					  and (tien_dnhm>0 or tien_sodep>0)
					  and ((loaitb_id not in (20,149)  and trangthai_tt_id=1 ) or loaitb_id in (20,149))
					  and (loai_ld not like '_LCN' or loai_ld is null)
					  and (nop_du=1 or mien_hsgoc is not null) 
					  and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
					
			;
			--32
                  
     


-- PGP
--		
				;
		--34
    
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt_nvhotro = 202409		---thang n
--		select thang_luong, thang_ptm, thang_tldg_dt, thang_tldg_dt_nvhotro, luong_dongia_nvptm, luong_dongia_nvhotro, manv_hotro, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202407 --thang n-2
					and nvl(thang_tldg_dt_nvhotro, 999999) >= 202409
					and (loaitb_id not in (210, 21,131) or ma_kh='GTGT rieng') and (chuquan_id is null or chuquan_id<>264)
					and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
					 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%')		---PGP dc tinh khi ban qua dai ly
										)
					and ( (thang_tldg_dt is null and lydo_khongtinh_luong like '%Phat trien qua Dai ly%')			---PGP dc tinh khi ban qua dai ly
										or thang_tldg_dt is not null				---tam thoi PTM < T5, sau do chuyen ve is not null sau >= T5
							)
					  and ( trangthaitb_id_n2=1 or (loaitb_id=20 and trangthaitb_id_n2=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
					  and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)>0 and nocuoc_n2 is null  
					  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
					  and (nop_du=1 or mien_hsgoc is not null) 
					
			;
            ---35
commit;
--*************** ky n-3 *********
--			
		--36



-- Dot 2: 
				update ttkd_bsc.ct_bsc_ptm a 
					   set thang_tldg_dt = 202409		--thang n
--				   select  chuquan_id, thang_ptm, dich_vu, ma_pb, ma_tb, ma_to, loai_ld, thang_tldg_dt, luong_dongia_nvptm, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a 
				    where thang_ptm = 202406		---thang n-3
								  and nvl(thang_tldg_dt, 999999) >= 202409
								  and (loaitb_id<>21 or ma_kh='GTGT rieng') and (chuquan_id in  (145,266) or chuquan_id is null)
								   and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
									) 
								   and not exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where loaitru_tinhluong=1 and loaitb_id=a.loaitb_id)
								  and (trangthaitb_id_n3=1 or (loaitb_id=20 and trangthaitb_id_n3=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146)))or loaitb_id in (89,90,146) )												  
								    and (nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)>0 
											or (nvl(dthu_ps_n2, 0) =0 and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
															and dthu_goi >0 and thang_bddc > a.thang_ptm		--thang n
												)
									) 
									and nocuoc_n3 is null					
								  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
								  and (nop_du=1 or mien_hsgoc is not null) 
								  and (loai_ld not like '_LCN' or loai_ld is null)
								  
					;
		--40

				update ttkd_bsc.ct_bsc_ptm a 
						set thang_tldg_dnhm = 202409		---thang n
--				    select chuquan_id, ma_tb, manv_ptm, ten_pb, thang_tldg_dt,  thang_tldg_dnhm, tien_dnhm, tien_tt, luong_dongia_dnhm_nvptm, thang_ptm, trangthai_tt_id, lydo_khongtinh_dongia from ttkd_bsc.ct_bsc_ptm a
				    where thang_ptm = 202406 		---thang n-3
							and nvl(thang_tldg_dnhm, 999999) >= 202409
							and (loaitb_id not in (21,210, 222) or ma_kh='GTGT rieng')  and chuquan_id in (145,266,264) 
							and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%' )
								)
							 and (tien_dnhm>0 or tien_sodep>0)
							 and ((loaitb_id not in (20,149)  and trangthai_tt_id=1 ) or loaitb_id in (20,149))
							 and (loai_ld not like '_LCN' or loai_ld is null)         
							 and exists (select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id )
							 and (nop_du=1 or mien_hsgoc is not null) 
							and nocuoc_n3 is null	
							
				;




-- PGP
			update ttkd_bsc.ct_bsc_ptm a 
			    set thang_tldg_dt_nvhotro = 202409		---thang n
--			    select thang_ptm, thang_tldg_dt, thang_tldg_dt_nvhotro, lydo_khongtinh_dongia, luong_dongia_nvptm, luong_dongia_nvhotro, manv_hotro from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm = 202406 	---thang n-3
							and nvl(THANG_TLDG_DT_NVHOTRO, 999999) >= 202409
							and (loaitb_id not in (21,131) or ma_kh='GTGT rieng') and chuquan_id in (145,266) 
							 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%')		---PGP dc tinh khi ban qua dai ly
												) 
							and ( (thang_tldg_dt is null and lydo_khongtinh_luong like '%Phat trien qua Dai ly%')			---PGP dc tinh khi ban qua dai ly
												or thang_tldg_dt is not null				---tam thoi PTM < T5, sau do chuyen ve is not null sau >= T5
									)
							  and ( trangthaitb_id_n3=1 or (loaitb_id=20 and trangthaitb_id_n3=10) or (thoihan_id=1 and (dichvuvt_id in (7,8,9) or loaitb_id in (1,58,59,39,146))) or loaitb_id in (89,90,146) )
							  and nvl(dthu_ps,0)+nvl(dthu_ps_n1,0)+nvl(dthu_ps_n2,0)+nvl(dthu_ps_n3,0)>0 and nocuoc_n3 is null
							  and ((loaitb_id not in (20,149) and trangthai_tt_id=1) or loaitb_id in (20,149))
							  and (nop_du=1 or mien_hsgoc is not null) 
							  and not exists (select 1 from ttkd_bsc.nhanvien where thang = a.thang_ptm and loai_ld like '_LCN' and ma_nv = a.manv_hotro)

			;
            
 commit; 
-- GTGT rieng thang n
		---log: thang 8 khong co rec
			update ttkd_bsc.ct_bsc_ptm a 
				   set  thang_tldg_dt = 202409, thang_tlkpi = 202409, thang_tlkpi_to = 202409, thang_tlkpi_phong = 202409
						 , thang_tldg_dt_nvhotro = case when manv_hotro is not null and thang_tldg_dt_nvhotro is null then 202409 end
--			     select * from ttkd_bsc.ct_bsc_ptm a 
			    where thang_ptm >= 202406 ---thang n-3
								and lydo_khongtinh_luong is null and thang_xacnhan_khkt is not null 
							   and thang_tldg_dt is null and ma_kh = 'GTGT rieng'
							   
			;
			

   
        
-- Tinh bs ho so gia han CA, IVAN,... dthu ps =0 nhung chua den ky dat coc moi: Chay dot 2 check lai
	---khong chay thang 5/2025, KIEMTRA xem da duoc tinh khong?
			update ttkd_bsc.ct_bsc_ptm a 
				   set thang_tldg_dt = 202409, thang_tlkpi = 202409, thang_tlkpi_to=202409, thang_tlkpi_phong=202409
						, thang_tldg_dt_nvhotro = 202409, thang_tlkpi_hotro = 202409
						--, thang_luong = 87
--			     select thang_luong, thang_ptm, thang_tldg_dt, thang_tldg_dt_nvhotro, ma_gd, ma_tb, dthu_goi, datcoc_csd, thang_bddc, doanhthu_dongia_nvptm, heso_daily, lydo_khongtinh_luong, lydo_khongtinh_dongia, dthu_ps from ttkd_bsc.ct_bsc_ptm a
			    where thang_ptm >= 202406 		---thang n-3
						and thang_bddc >= 202409			---thang n
						and nvl(thang_tldg_dt, 999999) >= 202409
							 and (lydo_khongtinh_luong is null or (lydo_khongtinh_luong like '%Phat trien qua Dai ly%')
									)
						   and loaitb_id in (55,80,116,117,140,132,122,288,181,290,292,175,302) 
			--	   and ma_tb = 'hcm_ca_00067906'
			;
			rollback;
			commit;
                
---- PGP - thieu hsg thang n
--				
--        
        
---- SMS Brn dot 1 ngay 5: khong chay dot 2 do chua co cuoc ps thang n+1
--			
--
commit;
  ----kiem tien dau noi hoa mang---
select thang_ptm, ma_gd, ma_tb, soseri, ngay_tt, tien_tt, tien_dnhm, trangthai_tt_id, chuquan_id, manv_ptm, MA_NGUOIGT
			, lydo_khongtinh_luong, heso_daily
  from ttkd_bsc.ct_bsc_ptm a
  where thang_tldg_dt=202407 and trangthai_tt_id=1 and (tien_dnhm>0 or tien_sodep >0) and ngay_tt is not null
    and exists(select 1 from ttkd_bsc.dm_loaihinh_hsqd where kieutinh_bsc_ptm='thang' and loaitb_id=a.loaitb_id)
    and thang_tldg_dnhm is null
    ;
    ----Kiem tra lydo_khongtinh_XXX co gia tri
    select thang_ptm, ma_gd, ma_tb, soseri, ngay_tt, tien_tt, tien_dnhm, trangthai_tt_id, chuquan_id, manv_ptm, MA_NGUOIGT
			, thang_bddc, lydo_khongtinh_luong, lydo_khongtinh_dongia, heso_daily, nop_du
  from ttkd_bsc.ct_bsc_ptm a
  where thang_tldg_dt=202407
					and (lydo_khongtinh_luong is not null or lydo_khongtinh_dongia is not null )
	;
	   ----Kiem tra lydo_khongtinh_XXX khong co gia tri
    select thang_ptm, ma_gd, nguon, ma_tb, thuebao_id, hdtb_id, dich_vu, trangthai_tt_id, chuquan_id, manv_ptm, ten_pb
			, thang_bddc, lydo_khongtinh_luong, lydo_khongtinh_dongia, heso_daily, nop_du, mien_hsgoc, trangthaitb_id, dthu_goi
			, THANG_TLDG_DT, THANG_TLKPI, THANG_TLKPI_TO, THANG_TLKPI_PHONG
  from ttkd_bsc.ct_bsc_ptm a
  where thang_tldg_dt is null and thang_ptm >= 202406
					and loaitb_id not in (21)
					and  lydo_khongtinh_dongia is null and manv_ptm is not null 
					and loai_ld not like '%LCN'		--dai ly ca nhan khong tinh don gia, chi tinh KPI
	;
	----Kiem tra du lieu imp duoc tinh hay chua
	  select 
				thang_ptm, ma_gd, ma_tb, soseri, ngay_tt, tien_tt, tien_dnhm, trangthai_tt_id, chuquan_id, manv_ptm, MA_NGUOIGT
			, thang_bddc, lydo_khongtinh_luong, lydo_khongtinh_dongia, heso_daily, nop_du
			, heso_hotro_nvhotro, thang_tldg_dt_nvhotro, thang_tldg_dt, dthu_goi, luong_dongia_nvptm, dthu_ps
  from ttkd_bsc.ct_bsc_ptm a
  where thang_luong in (87);
  
  ----Kiem tra cac truong hop có FLAG khong tinh luong
   select thang_luong, thang_ptm, ma_gd, nguon, ma_tb, thuebao_id, hdtb_id, dich_vu, trangthai_tt_id, chuquan_id, manv_ptm, ten_pb
			, thang_bddc, lydo_khongtinh_luong, lydo_khongtinh_dongia, heso_daily, nop_du, mien_hsgoc, trangthaitb_id, dthu_goi
			, THANG_TLDG_DT, THANG_TLKPI, THANG_TLKPI_TO, THANG_TLKPI_PHONG
  from ttkd_bsc.ct_bsc_ptm a
  where  thang_luong < 100
					and loaitb_id not in (21)
					and loai_ld not like '%LCN'		--dai ly ca nhan khong tinh don gia, chi tinh KPI
	;
  

