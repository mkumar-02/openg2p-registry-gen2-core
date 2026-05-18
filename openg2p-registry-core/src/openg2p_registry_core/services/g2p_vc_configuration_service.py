import logging
import uuid
from typing import List, Optional, Tuple

from openg2p_fastapi_common.service import BaseService
from openg2p_fastapi_common.context import dbengine

from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession
from sqlalchemy import func, select

from ..models import (
    G2PRegisterDefinition,
    G2PRegistryVcConfiguration
)
from ..schemas import (
    VcConfigurationData,
)
from ..errors import G2PRegistryErrorCodes, G2PRegistryException

_logger = logging.getLogger("g2p-outgestion-configuration-service")

class G2PVcConfigurationService(BaseService):

    async def get_vc_configuration_for_register(
        self,
        register_id: str,
        current_page: Optional[int] = None,
        page_size: Optional[int] = None,
    ) -> Tuple[List[VcConfigurationData], int]:
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            await self._validate_register_exists(register_id, session)
            _logger.info("validation Register exists")

            base_query = select(G2PRegistryVcConfiguration).where(
                G2PRegistryVcConfiguration.register_id == register_id
            )
            total_items = (
                await session.execute(
                    select(func.count()).select_from(base_query.subquery())
                )
            ).scalar_one()

            query = base_query
            if current_page is not None and page_size is not None:
                offset = (current_page - 1) * page_size
                query = query.offset(offset).limit(page_size)

            g2p_register_vc_configurations = (
                await session.execute(query)
            ).scalars().all()

            _logger.info(
                "Got %s vc configurations for register id %s (total=%s)",
                len(g2p_register_vc_configurations),
                register_id,
                total_items,
            )

            vc_configuration_data = [
                VcConfigurationData.model_validate(g2p_register_vc_configuration)
                for g2p_register_vc_configuration in g2p_register_vc_configurations
            ]
            return vc_configuration_data, total_items

    async def get_all_vc_configurations(
        self,
        current_page: Optional[int] = None,
        page_size: Optional[int] = None,
    ) -> Tuple[List[VcConfigurationData], int]:
        """Get all registry vc configurations."""
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            base_query = select(G2PRegistryVcConfiguration)
            total_items = (
                await session.execute(
                    select(func.count()).select_from(base_query.subquery())
                )
            ).scalar_one()

            query = base_query
            if current_page is not None and page_size is not None:
                offset = (current_page - 1) * page_size
                query = query.offset(offset).limit(page_size)

            g2p_register_vc_configurations = (
                await session.execute(query)
            ).scalars().all()

            _logger.info(
                "Got %s vc configurations (total=%s)",
                len(g2p_register_vc_configurations),
                total_items,
            )

            return [
                VcConfigurationData.model_validate(g2p_register_vc_configuration)
                for g2p_register_vc_configuration in g2p_register_vc_configurations
            ], total_items

    async def create_vc_configuration(
        self,
        register_id: str,
        vc_mnemonic: str,
        descriptor_schema: dict,
        intake_form_id: Optional[str] = None,
        data_model_id: Optional[str] = None,
    ) -> List[VcConfigurationData]:
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            await self._validate_register_exists(register_id, session)
            _logger.info("validation Register exists")

            g2p_register_vc_configuration = G2PRegistryVcConfiguration(
                vc_config_id=str(uuid.uuid4()),
                register_id=register_id,
                intake_form_id=intake_form_id,
                data_model_id=data_model_id,
                vc_mnemonic=vc_mnemonic,
                descriptor_schema=descriptor_schema,
            )
            session.add(g2p_register_vc_configuration)
            await session.commit()
    
            return [VcConfigurationData.model_validate(g2p_register_vc_configuration)]

    async def edit_descriptor_schema(
        self, 
        vc_config_id: str,
        descriptor_schema: dict
    ) -> List[VcConfigurationData]:
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            g2p_register_vc_configuration: G2PRegistryVcConfiguration = await self._get_vc_configuration(vc_config_id, session)

            g2p_register_vc_configuration.descriptor_schema = descriptor_schema
            
            await session.commit()
            await session.refresh(g2p_register_vc_configuration)
            return [VcConfigurationData.model_validate(g2p_register_vc_configuration)]

    async def delete_vc_configuration(
        self,
        vc_config_id: str,
    ) -> List[VcConfigurationData]:
        session_maker = async_sessionmaker(dbengine.get(), expire_on_commit=False)
        async with session_maker() as session:
            g2p_register_vc_configuration = await self._get_vc_configuration(vc_config_id, session)

            await session.delete(g2p_register_vc_configuration)
            await session.commit()
            return [VcConfigurationData.model_validate(g2p_register_vc_configuration)]


    async def _validate_register_exists(self, register_id: str, session: AsyncSession):
        """Validate that a register exists."""
        register_definition = await session.get(G2PRegisterDefinition, register_id)

        if not register_definition:
            raise G2PRegistryException(
                code=G2PRegistryErrorCodes.REGISTER_NOT_FOUND.value[1],
                message=f"Register with id {register_id} not found"
            )
    
    async def _get_vc_configuration(self, vc_config_id: str, session: AsyncSession) -> G2PRegistryVcConfiguration:
        g2p_register_vc_configuration = await session.get(G2PRegistryVcConfiguration, vc_config_id)

        if not g2p_register_vc_configuration:
            raise G2PRegistryException(
                code=G2PRegistryErrorCodes.VC_CONFIGURATION_NOT_FOUND.value[1],
                message=f"VC configuration with id {vc_config_id} not found"
            )
        return g2p_register_vc_configuration