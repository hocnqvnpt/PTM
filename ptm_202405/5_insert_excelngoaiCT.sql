select max(thang_luong) from ct_bsc_ptm where thang_luong<100 group by thang_luong;

create table a_ptm_ngoaictr_imp as select a.*, cast(null as varchar2(500)) thuchien from hocnq_ttkd.a_ptm_ngoaictr_imp a;

-- 1. Hop dong da co trong ct_bsc_ptm
select * from ttkd_bsc.a_ptm_ngoaictr_imp a
where exists(select 1 from ttkd_bsc.ct_bsc_ptm where ma_gd=trim(a.ma_gd) and ma_tb=trim(a.ma_tb))
and dichvu_vt<>'VNP tr? sau'
    and thuchien is null;

update ttkd_bsc.ct_bsc_ptm a
    set (ghi_chu,DTHU_GOI, HESO_DICHVU, HESO_TRATRUOC, HESO_KHACHHANG, thang_luong) =
                (select dien_giai, DTHU_GOI, HESO_DICHVU, HESO_TRATRUOC, HESO_KHACHHANG,4
                    from a_ptm_ngoaictr_imp
                    where trim(ma_gd)=a.ma_gd and trim(ma_tb)=a.ma_tb and dichvu_vt<>'VNP tr? sau')                                                           
    -- select * from ttkd_bsc.ct_bsc_ptm a
    where ma_gd='HCM-LD/01414150';
                        
update ttkd_bsc.a_ptm_ngoaictr_imp set thuchien='Old: dthu_goi=0; New: dieu chinh dthu goi theo de nghi nhom TL va tinh lai'
    where  ma_gd='HCM-TD/00657435';
    

update ttkd_bsc.a_ptm_ngoaictr_imp set thuchien='Da nhan anh Nghia ko tinh vi ptm qua Dai ly DL_CNT'
    where  ma_gd='HCM-LD/01380021';
    
update ttkd_bsc.a_ptm_ngoaictr_imp set thuchien='01/2024 da tinh dthu_goi=1700000. Tinh them dthu goi theo de nghi cua nhom TL'
    where ma_gd='HCM-DV/10280947';
    
update ttkd_bsc.a_ptm_ngoaictr_imp set thuchien='Voice brn ptm 08/2023. Tinh theo de nghi cua nhom TL'
    where ma_gd='HCM-LD/01414150';
    
update ttkd_bsc.a_ptm_ngoaictr_imp set manv_ptm='VNP053485'
    where manv_ptm='VNPT053485' ;
    

-- 2. Hop dong chua co trong ttkd_bsc.ct_bsc_ptm
select (select loaitb_id from ttkd_bsc.dm_loaihinh_hsqd where loaihinh_tb=a.dichvu_vt) loaitb_id, a.*
from ttkd_bsc.a_ptm_ngoaictr_imp a
where not exists(select 1 from ttkd_bsc.ct_bsc_ptm where nvl(ma_gd,' ')=nvl(a.ma_gd,' ') and nvl(ma_tb,' ')=nvl(a.ma_tb,' '))
and dichvu_vt not in ('VNP tr? sau','VCC')
    and thuchien is null;

select * from ttkd_bsc.dm_loaihinh_hsqd where loaihinh_tb like '%FMS';

update ttkd_bsc.a_ptm_ngoaictr_imp a
    set dichvu_vt = 'VNEdu FMS' 
    where dichvu_vt='Vnedu FMS';
    
    
insert into ttkd_bsc.ct_bsc_ptm a     
           (thang_luong, thang_ptm, dich_vu, tenkieu_ld, ma_gd, ma_tb, ma_kh, sohopdong, ten_tb, ngay_bbbg, dthu_goi
            ,dichvuvt_id, loaitb_id,heso_dichvu, heso_tratruoc, heso_khachhang, heso_hoso, tyle_hotro, heso_hotro_nvhotro, manv_hotro, ghi_chu, chuquan_id
            ,nguon, heso_vtcv_nvptm, heso_vtcv_nvhotro, heso_hotro_nvptm, heso_quydinh_nvptm, heso_quydinh_nvhotro, dongia
            ,pbh_ptm_id, manv_ptm, tennv_ptm, ma_to,ten_to,ma_pb,ten_pb,ma_vtcv,loainv_id,loai_ld,nhom_tiepthi, trangthai_tt_id
            ,thang_tldg_dt, thang_tlkpi, thang_tlkpi_to, thang_tlkpi_phong)
select 5, 202403, dichvu_vt, tenkieu_ld, ma_gd, ma_tb, ma_kh, so_hd, ten_kh, ngay_yc, dthu_goi
            ,(select dichvuvt_id from ttkd_bsc.dm_loaihinh_hsqd where loaihinh_tb=a.dichvu_vt) dichvuvt_id
            ,(select loaitb_id from ttkd_bsc.dm_loaihinh_hsqd where loaihinh_tb=a.dichvu_vt) loaitb_id
            ,heso_dichvu, heso_tratruoc, decode(heso_khachhang,null,1,heso_khachhang), heso_hoso, heso_hotro_nvhotro, heso_hotro_nvhotro, manv_hotro, dien_giai, 145
            ,case when TRONG_CT=1 then 'Trong co che tinh luong' else 'Ngoai co che tinh luong' end,1, 1, 1, 1, 1, 858
            ,b.pbh_id pbh_ptm_id, a.MANV_PTM, b.ten_nv tennv_ptm, b.ma_to,b.ten_to,b.ma_pb,b.ten_pb,b.ma_vtcv,b.loainv_id,b.loai_ld,b.nhomld_id, 1
            ,case when ma_gd like 'HCM-TT%' and (select dichvuvt_id from ttkd_bsc.dm_loaihinh_hsqd where loaihinh_tb=a.dichvu_vt) in (4,7,8,9)
                    then 202403 end thang_tldg_dt
            ,case when ma_gd like 'HCM-TT%' and exists(select dichvuvt_id from ttkd_bsc.dm_loaihinh_hsqd where dichvuvt_id in (4,7,8,9) and loaihinh_tb=a.dichvu_vt)  then 999999 end thang_tlkpi
            ,case when ma_gd like 'HCM-TT%' and exists(select dichvuvt_id from ttkd_bsc.dm_loaihinh_hsqd where dichvuvt_id in (4,7,8,9) and loaihinh_tb=a.dichvu_vt) then 999999 end thang_tlkpi_to
            ,case when ma_gd like 'HCM-TT%' and exists(select dichvuvt_id from ttkd_bsc.dm_loaihinh_hsqd where dichvuvt_id in (4,7,8,9) and loaihinh_tb=a.dichvu_vt) then 999999 end thang_tlkpi_phong

from ttkd_bsc.a_ptm_ngoaictr_imp a, 
            (select pb.pbh_id,b.ma_nv,b.ten_nv,b.ma_to,b.ten_to,b.ma_pb,b.ten_pb,b.ma_vtcv,vtcv.loainv_id,vtcv.ten,b.loai_ld, nhomld_id
            from ttkd_bsc.nhanvien_202403 b 
                    left join ttkd_bsc.dm_phongban pb on b.ma_pb=pb.ma_pb
                    left join (select distinct c.ma_vtcv, d.loainv_id, d.ten from ttkd_bsc.dm_vtcv c, ttkd_bsc.loai_nv d
                                      where c.loainv_id=d.loainv_id) vtcv on b.ma_vtcv=vtcv.ma_vtcv) b
where a.manv_ptm=b.ma_nv(+) --and dichvu_vt = 'VNEdu FMS' 
    and not exists(select 1 from ttkd_bsc.ct_bsc_ptm where nvl(trim(ma_gd),' ')=nvl(trim(a.ma_gd),' ') and nvl(trim(ma_tb),' ')=nvl(trim(a.ma_tb),' '))
    and (select loaitb_id from ttkd_bsc.dm_loaihinh_hsqd where loaihinh_tb=a.dichvu_vt) not in (20,149) 
    and a.thuchien is null;

select * from ttkd_bsc.a_ptm_ngoaictr_imp a;   