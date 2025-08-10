<script>
    import { onMount } from 'svelte';

    let predictions = [];
    let modelInfo = null;
    let loading = true;
    let error = null;

    onMount(async () => {
        try {
            const res = await fetch('http://127.0.0.1:8000/qualifying'); // your backend URL
            if (!res.ok) throw new Error("Failed to fetch data");
            const data = await res.json();
            predictions = data.predictions;
            modelInfo = data.model_info;
        } catch (err) {
            error = err.message;
        } finally {
            loading = false;
        }
    });
</script>

<div class="grid-page absolute inset-0 z-[-1]">
    <h1>🏁 Predicted Grid</h1>

    {#if loading}
        <p>Loading...</p>
    {:else if error}
        <p style="color: red;">{error}</p>
    {:else}
        <div class="model-info">
            <h2>Model Info</h2>
            <p><strong>Type:</strong> {modelInfo.type}</p>
            <p><strong>MAE:</strong> {modelInfo.mae}</p>
            <h3>Hyperparameters:</h3>
            <ul>
                {#each Object.entries(modelInfo.hyperparameters) as [key, value]}
                    <li><strong>{key}:</strong> {value}</li>
                {/each}
            </ul>
        </div>

        <table>
            <thead>
                <tr>
                    <th>Position</th>
                    <th>Driver</th>
                    <th>Team</th>
                </tr>
            </thead>
            <tbody>
                {#each predictions as p}
                    <tr>
                        <td>{p.position}</td>
                        <td>{p.driver}</td>
                        <td>{p.team}</td>
                    </tr>
                {/each}
            </tbody>
        </table>
    {/if}
</div>

<style>
    .grid-page {
        padding: 2rem;
        background: #111;
        color: white;
        font-family: sans-serif;
        min-height: 100vh;
    }
    h1 {
        color: #e10600;
    }
    table {
        width: 100%;
        border-collapse: collapse;
        margin-top: 1.5rem;
        background: #222;
    }
    th, td {
        padding: 0.8rem;
        border: 1px solid #333;
        text-align: left;
    }
    th {
        background: #e10600;
    }
    .model-info {
        margin-top: 1rem;
        padding: 1rem;
        background: #1a1a1a;
        border-radius: 5px;
    }
</style>
