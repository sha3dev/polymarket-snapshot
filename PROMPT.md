Read these files before making any implementation changes:

- `AGENTS.md`
- `ai/contract.json`
- `ai/rules.md`
- `prompts/init-contract.md`
- the assistant-specific adapter in `ai/`

Your job is to implement the requested behavior in the scaffold under `src/` and `test/` following the rules in `ai/rules.md` and `prompts/init-contract.md`.

## Package Specification

- Goal:
- Public API:
- Runtime constraints:
- Required dependencies:
- Feature requirements:

## Non-Negotiables

- You MUST execute `npm run check` yourself before finishing.
- If `npm run check` fails, you MUST fix the issues and rerun it until it passes.
- You MUST implement the task without editing managed files unless this is a standards update.

## Implementation Request

Complete this section before sending the prompt to your LLM.
Describe the behavior you want to implement, the expected public API, any runtime constraints, and any non-goals.

Task:

El objetivo de esta lib es el de crear un flujo de snapshot de polymarket cada n milisegundos.

Para ello hará uso de dos paquetes:

- @sha3/polymarket: para obtener los datos de los mercados de polymarket
- @sha3/cryptography:para obtener los datos de los mercados crypto

Un snapshot es como una "foto" del estado de actual de los mercados 5m/15m de polymarket, para cualquiera de sus assets (btc,eth,etc.). Estas fotos
las usaremos para un futuro proceso de entreno/prediccion con tensorflow.

Estas fotos incluyen toda la indformación del precio, order books, tanto del mercado de polimarket como del mercado de crypto (de todos los sources disponibles)

La idea es que yo pueda hacer un "addSnapshotListener({windows: ["5m"], assets: ["btc"]})" y que cada 500ms me vaya devolviendo un snapshot que contenga toda la información
relacionada, con esos mercados. Es muy posible que durante esa ventana de 500ms lleguen varios eventos del mismo tipo, por ejemplo, el precio puede cambiar varias veces. El valor que
que debe devolver cada snapshot es siempre el mas nuevo. De igual forma, es posible que no tengamos updates de algun dato, en ese caso el snapshot debe devolver el valor mas reciente tambien
(aunque no se haya actualizado duante los ultimos 500ms). La función addSnapshotListener admite dos parfametros, los dos opcionales. Si no tenemos valor (esto pasará al inicio) el valor debe ser null.
Por ejemplo, si aun no tenemos el precio de BTC, lo marcaremos como null. Internamente deberiamos mantener como dos estructuras diferentes. Una para crypto y otra para los mercados de polymarket.
Adems del addSnapshotListener quiero un removeSnapshotListener, y un metodo que devuelve el snapshot mas reciente (getSnapshot(...)).

IMPORTANTE: los slugs de mercados van cambiando en polymarket, cad 5m/15m se crean mercados nuevos, tendremos que ir subscribieno/dessuscribiendonos para mantener un flujo continuo de snapshots.

IMPORTANTE: los eventos generados por un mercado, antes o despues de su hora de inicio/fin, los descartamos. Los mercados en polymarket (5m/15) tienen una hora de fin, pero pasada esa hora de fin aun pueden generar eventos residuales. Hay que desvcartarlos.

IMPORTANTE: Hay un dato que TAMBIEN necesitamos, y es el priceToBeat. Ese dato lo puedes conseguir con @sha3/polymarket, y deberia estar presente en el snapshot. Ese dato, no cambia durante el trans
cusrso de la ventana, así que una vez obtenido no se deberñia volver a pedir. Ese deato puede que no esté disponible justo al inicio de le ventana. Lo que haremos es pedirlo al cabo de un un delay, y si
no lo podemos conseguir, hacer un reintento cada n segundos.

Quiero que analices el problema, analices las librerias crypto y polymarket, y planifiques la mejor forma de conseghuir lo que te pido, de la forma mas optima posible. Manteniendo
las estrucutras mas óptimas en memoria, y haciendo las mínimas llamadas posibles.

IMPORTANTE: cada snapshot, debe contenr tambien la info de a que asset/window pertenece.




